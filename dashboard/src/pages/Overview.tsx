import { useOverview, useQueueForecast } from "../api/hooks";
import { RANGE_MINUTES, sinceMs, useApp } from "../AppContext";
import { KpiStrip } from "../components/KpiStrip";
import { QueueChart } from "../components/QueueChart";
import { Topology } from "../components/Topology";
import { Ring } from "../components/ui/charts";
import { Card, Guard, PanelState } from "../components/ui/primitives";
import { history } from "../history";
import { useMarkers } from "../stream/store";

const ALERT_KINDS = new Set(["chaos", "failure", "election", "drift", "scale", "admin"]);

export function OverviewPage() {
  const { range } = useApp();
  const overview = useOverview();
  const forecast = useQueueForecast(RANGE_MINUTES[range]);
  const since = sinceMs(range);
  const markers = useMarkers(since);
  return (
    <div className="space-y-4">
      <Guard query={overview}>{(o) => <KpiStrip overview={o} history={history.kpis.filter((k) => k.t >= since)} />}</Guard>
      <div className="grid gap-4 xl:grid-cols-3">
        <Card title="Cluster topology" className="xl:col-span-2">
          <Guard query={overview}>
            {(o) => (o.cluster ? <Topology cluster={o.cluster} /> : <PanelState kind="error" message={o.clusterError} />)}
          </Guard>
        </Card>
        <Card title="Busy slots">
          <Guard query={overview}>
            {(o) => {
              const slots = o.cluster?.workers.reduce((s, w) => s + (w.healthy ? w.poolSize : 0), 0) ?? 0;
              const busy = o.cluster?.workers.reduce((s, w) => s + (w.healthy ? w.activeThreads : 0), 0) ?? 0;
              return (
                <div className="flex flex-col items-center gap-2 py-2" data-testid="gauge">
                  <Ring value={slots ? (100 * busy) / slots : 0} label={`${busy} of ${slots} pool slots running`}
                    color="var(--accent)" size={150} />
                  <p className="tabular text-sm text-muted">
                    {o.cluster ? `${o.cluster.arrivalRatePerSec.toFixed(1)} submits/s · decision p95 ${o.cluster.decisionP95Ms.toFixed(2)} ms` : ""}
                  </p>
                </div>
              );
            }}
          </Guard>
        </Card>
      </div>
      <div className="grid gap-4 xl:grid-cols-3">
        <Card title="Live queue: actual vs predicted" className="xl:col-span-2">
          <Guard query={forecast} empty={(f) => !f.actual.length && !f.predicted.length}>
            {(f) => <QueueChart actual={f.actual} predicted={f.predicted} markers={markers} />}
          </Guard>
        </Card>
        <Card title="Alerts">
          {markers.filter((m) => ALERT_KINDS.has(m.kind)).length === 0 ? (
            <PanelState kind="empty" message="No elections, failures or chaos in this range" />
          ) : (
            <ul className="max-h-64 space-y-1 overflow-auto text-sm" data-testid="alerts">
              {[...markers].reverse().map((m, i) => (
                <li key={i} className="flex justify-between gap-2 border-b border-rule py-1">
                  <span className="font-medium">{m.label}</span>
                  <span className="tabular text-muted">{new Date(m.t).toLocaleTimeString([], { hour12: false })}</span>
                </li>
              ))}
            </ul>
          )}
        </Card>
      </div>
    </div>
  );
}
