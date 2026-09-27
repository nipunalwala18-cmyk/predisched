import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { postJson } from "../api/client";
import { useAccuracy, useModels, useQueueForecast } from "../api/hooks";
import type { Row } from "../api/types";
import { RANGE_MINUTES, sinceMs, useApp } from "../AppContext";
import { MaeOverTime, PredVsActual } from "../components/PredictionCharts";
import { QueueChart } from "../components/QueueChart";
import { Button, Card, Guard, PanelState } from "../components/ui/primitives";
import { useMarkers } from "../stream/store";

function liveTestMae(models?: Record<string, Row>): number | null {
  const m1 = models?.m1;
  if (!m1) return null;
  const live = (m1.versions as Row[]).find((v) => v.status === "live");
  return live?.metrics?.test?.mae ?? null;
}

export function PredictionsPage() {
  const { range } = useApp();
  const accuracy = useAccuracy();
  const models = useModels();
  const forecast = useQueueForecast(RANGE_MINUTES[range]);
  const client = useQueryClient();
  const [promotion, setPromotion] = useState<string | null>(null);
  const markers = useMarkers(sinceMs(range));

  const promote = async (model: string, version: number) => {
    const r = await postJson<Row>(`/api/models/${version}/promote?model=${model}`, undefined, true);
    setPromotion(String(r.output ?? (r.promoted ? "promoted" : "refused")));
    client.invalidateQueries({ queryKey: ["models"] });
  };

  return (
    <div className="space-y-4">
      <div className="grid gap-4 xl:grid-cols-2">
        <Card title="Predicted vs actual execution time">
          <Guard query={accuracy} empty={(a) => !a.recent.length}>{(a) => <PredVsActual rows={a.recent} />}</Guard>
        </Card>
        <Card title="Rolling MAE (drift threshold dashed)">
          <Guard query={accuracy} empty={(a) => !a.recent.length}>
            {(a) => <MaeOverTime rows={a.recent} testMae={liveTestMae(models.data?.models)} markers={markers} />}
          </Guard>
        </Card>
      </div>
      <div className="grid gap-4 xl:grid-cols-3">
        <Card title="Queue forecast vs actual" className="xl:col-span-2">
          <Guard query={forecast} empty={(f) => !f.actual.length}>
            {(f) => <QueueChart actual={f.actual} predicted={f.predicted} markers={markers} />}
          </Guard>
        </Card>
        <Card title="Error by task type (MAE, ms)">
          <Guard query={accuracy} empty={(a) => !a.byType.length}>
            {(a) => (
              <table className="tabular w-full text-sm" data-testid="mae-by-type">
                <thead className="text-xs text-muted"><tr><th className="text-left">Type</th><th>n</th><th>MAE</th><th>Cold</th></tr></thead>
                <tbody>
                  {a.byType.map((t) => (
                    <tr key={String(t.task_type)} className="border-b border-rule">
                      <td>{String(t.task_type).replace("_TASK", "")}</td>
                      <td className="text-center">{t.n}</td>
                      <td className="text-center">{Number(t.mae_ms).toFixed(1)}</td>
                      <td className="text-center">{a.recent.filter((r) => r.task_type === t.task_type && r.cold_start).length}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </Guard>
        </Card>
      </div>
      <div className="grid gap-4 xl:grid-cols-3">
        <Card title="Model registry: live, shadow, retired" className="xl:col-span-2">
          <Guard query={models} empty={(m) => !Object.keys(m.models).length}>
            {(m) => (
              <div className="space-y-2" data-testid="registry">
                <table className="tabular w-full text-sm">
                  <thead className="text-xs text-muted">
                    <tr><th className="text-left">Model</th><th>Version</th><th>Status</th><th className="text-left">Kept</th><th>Test metric</th><th /></tr>
                  </thead>
                  <tbody>
                    {Object.entries(m.models).flatMap(([name, entry]) => (entry.versions as Row[]).map((v) => (
                      <tr key={`${name}-${v.version}`} className="border-b border-rule">
                        <td>{name}</td><td className="text-center">v{v.version}</td>
                        <td className="text-center" style={{ color: v.status === "live" ? "var(--good)" : v.status === "shadow" ? "var(--warn)" : "var(--muted)" }}>{v.status}</td>
                        <td className="max-w-[240px] truncate">{v.kept}</td>
                        <td className="text-center">{v.metrics?.test?.mae != null ? `MAE ${Number(v.metrics.test.mae).toFixed(2)}` : v.metrics?.test?.f1 != null ? `F1 ${Number(v.metrics.test.f1).toFixed(3)}` : "–"}</td>
                        <td className="text-right">
                          {v.status === "shadow" && <Button size="sm" onClick={() => promote(name, Number(v.version))}>Promote</Button>}
                        </td>
                      </tr>
                    )))}
                  </tbody>
                </table>
                {promotion && <p className="text-sm" role="status">{promotion}</p>}
                {m.drift.length > 0 && <p className="text-sm" style={{ color: "var(--warn)" }}>Last drift: {String(m.drift[0].ts)}</p>}
              </div>
            )}
          </Guard>
        </Card>
        <Card title="Feature importance · overload risk heatmap">
          <PanelState kind="notbuilt" message="Not built yet: see ml/reports/*-feature-importance.png (no API endpoint)" />
        </Card>
      </div>
    </div>
  );
}
