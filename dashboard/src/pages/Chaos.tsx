import { useOverview } from "../api/hooks";
import { ChaosPanel } from "../components/ChaosPanel";
import { Card, Guard, PanelState } from "../components/ui/primitives";
import { stream, useStreamVersion } from "../stream/store";

export function ChaosPage() {
  useStreamVersion();
  const overview = useOverview();
  const chaos = stream.markers.filter((m) => m.kind !== "admin");
  return (
    <div className="space-y-4">
      <Card title="Chaos controls (each asks for confirmation; every action marks the charts)">
        <Guard query={overview}>
          {(o) => (
            <ChaosPanel workers={o.cluster?.workers ?? []}
              schedulers={(o.cluster?.nodes ?? []).filter((n) => n.reachable).map((n) => n.id)} />
          )}
        </Guard>
      </Card>
      <Card title="Recent faults and elections">
        {chaos.length === 0 ? <PanelState kind="empty" message="Nothing injected in the last five minutes" /> : (
          <ul className="space-y-1 text-sm" data-testid="chaos-log">
            {[...chaos].reverse().map((m, i) => (
              <li key={i} className="flex justify-between border-b border-rule py-1">
                <span><b>{m.kind}</b> · {m.label}</span>
                <span className="tabular text-muted">{new Date(m.t).toLocaleTimeString([], { hour12: false })}</span>
              </li>
            ))}
          </ul>
        )}
      </Card>
    </div>
  );
}
