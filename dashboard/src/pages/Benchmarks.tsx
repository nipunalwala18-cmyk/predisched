import { useMemo, useState } from "react";
import { API_URL, postJson } from "../api/client";
import { useBenchmarks } from "../api/hooks";
import type { Row } from "../api/types";
import { StrategyBars, StrategyRadar, suiteCells } from "../components/BenchmarkCharts";
import { Button, Card, Guard, PanelState } from "../components/ui/primitives";

const METRICS: [string, string][] = [
  ["mean_latency_ms", "Mean latency (ms)"],
  ["p95_latency_ms", "p95 latency (ms)"],
  ["throughput_per_s", "Throughput (tasks/s)"],
  ["sla_violation_pct", "SLA violations (%)"],
  ["imbalance", "Imbalance"],
];

export function BenchmarksPage() {
  const benchmarks = useBenchmarks();
  const suites = benchmarks.data?.suites ?? [];
  const [suite, setSuite] = useState<string>("");
  const [metric, setMetric] = useState(METRICS[0][0]);
  const chosen = suite || String(suites.find((s) => Number(s.runs) >= 25)?.suite ?? suites[0]?.suite ?? "");
  const cells = useMemo(() => suiteCells(benchmarks.data?.runs ?? [], chosen), [benchmarks.data, chosen]);

  const [sim, setSim] = useState<Row | null>(null);
  const [simForm, setSimForm] = useState({ strategy: "least_loaded", speed: 10,
    trace: "workloads/benchmark/heterogeneous-mixed-steady-2003.jsonl", workers: "heterogeneous" });
  const [simBusy, setSimBusy] = useState(false);
  const simulate = async () => {
    setSimBusy(true);
    try { setSim(await postJson<Row>("/api/simulate", simForm)); } finally { setSimBusy(false); }
  };

  return (
    <div className="space-y-4">
      <Card title={`Strategy comparison${chosen ? ` · suite ${chosen}` : ""}`} action={
        <div className="flex gap-2 text-sm">
          <select aria-label="suite" className="h-8 rounded border border-rule bg-panel px-2" value={chosen} onChange={(e) => setSuite(e.target.value)}>
            {suites.map((s) => <option key={String(s.suite)} value={String(s.suite)}>{String(s.suite)} ({s.runs} runs)</option>)}
          </select>
          <select aria-label="metric" className="h-8 rounded border border-rule bg-panel px-2" value={metric} onChange={(e) => setMetric(e.target.value)}>
            {METRICS.map(([k, l]) => <option key={k} value={k}>{l}</option>)}
          </select>
        </div>}>
        <Guard query={benchmarks} empty={() => !cells.length}>
          {() => <StrategyBars cells={cells} metric={metric} label={METRICS.find((m) => m[0] === metric)?.[1] ?? metric} />}
        </Guard>
      </Card>
      <div className="grid gap-4 xl:grid-cols-2">
        <Card title="Overall profile (normalised per scenario)">
          {cells.length ? <StrategyRadar cells={cells} /> : <PanelState kind="empty" />}
        </Card>
        <Card title="Latency CDF overlay">
          <PanelState kind="notbuilt" message="Not built yet: needs per-task latencies the API does not serve; the suite report has it" />
          <div className="flex flex-wrap justify-center gap-3 text-sm">
            {(benchmarks.data?.reports ?? []).map((r) => (
              <a key={r} href={`${API_URL.replace(/:\d+$/, ":8080")}/${r}`} className="text-accent underline" target="_blank" rel="noreferrer">{r}</a>
            ))}
          </div>
        </Card>
      </div>
      <div className="grid gap-4 xl:grid-cols-2">
        <Card title="Run history">
          <Guard query={benchmarks} empty={(b) => !b.suites.length}>
            {(b) => (
              <table className="tabular w-full text-sm" data-testid="run-history">
                <thead className="text-xs text-muted"><tr><th className="text-left">Suite</th><th>Runs</th><th>Started</th><th>Finished</th></tr></thead>
                <tbody>
                  {b.suites.map((s) => (
                    <tr key={String(s.suite)} className="border-b border-rule">
                      <td>{s.suite}</td><td className="text-center">{s.runs}</td>
                      <td className="text-center">{new Date(s.started).toLocaleString()}</td>
                      <td className="text-center">{new Date(s.finished).toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </Guard>
        </Card>
        <Card title="What-if simulator (a model, not a measurement)">
          <div className="grid grid-cols-2 gap-2 text-sm">
            <label>Strategy
              <select className="mt-1 w-full rounded border border-rule bg-panel p-1.5" value={simForm.strategy}
                onChange={(e) => setSimForm({ ...simForm, strategy: e.target.value })}>
                {["round_robin", "random", "least_loaded", "resource_aware", "predictive"].map((s) => <option key={s}>{s}</option>)}
              </select>
            </label>
            <label>Workers
              <select className="mt-1 w-full rounded border border-rule bg-panel p-1.5" value={simForm.workers}
                onChange={(e) => setSimForm({ ...simForm, workers: e.target.value })}>
                {["heterogeneous", "homogeneous", "single"].map((s) => <option key={s}>{s}</option>)}
              </select>
            </label>
            <label className="col-span-2">Trace
              <input className="mt-1 w-full rounded border border-rule bg-panel p-1.5" value={simForm.trace}
                onChange={(e) => setSimForm({ ...simForm, trace: e.target.value })} />
            </label>
          </div>
          <Button className="mt-3" onClick={simulate} disabled={simBusy}>{simBusy ? "Simulating…" : "Simulate at 10×"}</Button>
          {sim?.metrics && (
            <dl className="tabular mt-3 grid grid-cols-2 gap-1 text-sm" data-testid="sim-result">
              {Object.entries(sim.metrics as Record<string, number>).map(([k, v]) => (
                <div key={k} className="contents"><dt className="text-muted">{k}</dt><dd>{Number(v).toFixed(1)}</dd></div>
              ))}
            </dl>
          )}
          {sim?.error && <p className="mt-2 text-sm" style={{ color: "var(--bad)" }}>{String(sim.error)}</p>}
        </Card>
      </div>
    </div>
  );
}
