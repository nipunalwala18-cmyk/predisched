import {
  Bar, BarChart, CartesianGrid, Legend, PolarAngleAxis, PolarGrid, Radar, RadarChart,
  ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import type { Row } from "../api/types";
import { STRATEGY_COLORS, STRATEGY_LABELS } from "../theme/palette";

export interface SuiteCell {
  scenario: string;
  strategy: string;
  n: number;
  metrics: Record<string, number>;
}

/** benchmark_runs rows of one suite as mean metrics per scenario × strategy. */
export function suiteCells(runs: Row[], suite: string): SuiteCell[] {
  const cells = new Map<string, { scenario: string; strategy: string; values: Record<string, number[]> }>();
  for (const r of runs) {
    const [, s, scenario] = String(r.scenario).split(":");
    if (s !== suite) continue;
    let m: Record<string, unknown> = {};
    try { m = JSON.parse(String(r.metrics)); } catch { continue; }
    const key = `${scenario}|${r.strategy}`;
    const cell = cells.get(key) ?? { scenario, strategy: String(r.strategy), values: {} };
    for (const [k, v] of Object.entries(m)) {
      if (typeof v === "number" && Number.isFinite(v)) (cell.values[k] ??= []).push(v);
    }
    cells.set(key, cell);
  }
  return [...cells.values()].map((c) => ({
    scenario: c.scenario,
    strategy: c.strategy,
    n: Math.max(0, ...Object.values(c.values).map((v) => v.length)),
    metrics: Object.fromEntries(Object.entries(c.values).map(([k, v]) => [k, v.reduce((a, b) => a + b, 0) / v.length])),
  }));
}

/** Grouped bars: one group per scenario, one bar per strategy, for a chosen metric (§15.6). */
export function StrategyBars({ cells, metric, label }: { cells: SuiteCell[]; metric: string; label: string }) {
  const scenarios = [...new Set(cells.map((c) => c.scenario))];
  const strategies = [...new Set(cells.map((c) => c.strategy))];
  const data = scenarios.map((s) => {
    const row: Record<string, number | string> = { scenario: s };
    for (const st of strategies) {
      const c = cells.find((x) => x.scenario === s && x.strategy === st);
      if (c && c.metrics[metric] !== undefined) row[st] = Number(c.metrics[metric].toFixed(2));
    }
    return row;
  });
  return (
    <div className="h-80 w-full" data-testid="strategy-bars">
      <ResponsiveContainer>
        <BarChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }} barGap={2}>
          <CartesianGrid vertical={false} />
          <XAxis dataKey="scenario" fontSize={12} />
          <YAxis fontSize={11} width={60} label={{ value: label, angle: -90, position: "insideLeft", fill: "var(--muted)", fontSize: 11 }} />
          <Tooltip contentStyle={{ background: "var(--panel)", border: "1px solid var(--rule)" }} />
          <Legend />
          {strategies.map((st) => (
            <Bar key={st} dataKey={st} name={STRATEGY_LABELS[st] ?? st} fill={STRATEGY_COLORS[st] ?? "var(--accent)"}
              radius={[4, 4, 0, 0]} isAnimationActive={false} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

/** Five normalised axes per strategy (1 = best within a scenario, averaged over scenarios). */
export function StrategyRadar({ cells }: { cells: SuiteCell[] }) {
  const axes: [string, string, boolean][] = [
    ["mean_latency_ms", "Latency", true], ["throughput_per_s", "Throughput", false],
    ["imbalance", "Balance", true], ["sla_violation_pct", "SLA", true], ["overhead_mean_us", "Overhead", true],
  ];
  const strategies = [...new Set(cells.map((c) => c.strategy))];
  const scenarios = [...new Set(cells.map((c) => c.scenario))];
  const data = axes.map(([metric, name, lower]) => {
    const row: Record<string, number | string> = { axis: name };
    for (const st of strategies) {
      const scores: number[] = [];
      for (const sc of scenarios) {
        const vals = strategies.map((s) => cells.find((c) => c.scenario === sc && c.strategy === s)?.metrics[metric]);
        const mine = cells.find((c) => c.scenario === sc && c.strategy === st)?.metrics[metric];
        const finite = vals.filter((v): v is number => v !== undefined);
        if (mine === undefined || !finite.length) continue;
        const lo = Math.min(...finite);
        const hi = Math.max(...finite);
        scores.push(hi === lo ? 1 : lower ? 1 - (mine - lo) / (hi - lo) : (mine - lo) / (hi - lo));
      }
      row[st] = scores.length ? Number((scores.reduce((a, b) => a + b, 0) / scores.length).toFixed(2)) : 0;
    }
    return row;
  });
  return (
    <div className="h-80 w-full" data-testid="radar">
      <ResponsiveContainer>
        <RadarChart data={data} outerRadius="70%">
          <PolarGrid />
          <PolarAngleAxis dataKey="axis" fontSize={12} />
          <Tooltip contentStyle={{ background: "var(--panel)", border: "1px solid var(--rule)" }} />
          <Legend />
          {strategies.map((st) => (
            <Radar key={st} dataKey={st} name={STRATEGY_LABELS[st] ?? st} stroke={STRATEGY_COLORS[st]}
              fill={STRATEGY_COLORS[st]} fillOpacity={0.08} strokeWidth={2} isAnimationActive={false} />
          ))}
        </RadarChart>
      </ResponsiveContainer>
    </div>
  );
}
