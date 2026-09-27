import { useState } from "react";
import { Card, PanelState } from "../components/ui/primitives";
import { stream, useStreamVersion } from "../stream/store";

/** The unified event stream (spec §15.8) with trace search and a Lamport-order view. */
export function LogsPage() {
  useStreamVersion();
  const [search, setSearch] = useState("");
  const [node, setNode] = useState("");
  const [type, setType] = useState("");
  const [lamport, setLamport] = useState(false);
  const all = [...stream.events, ...stream.decisions];
  const nodes = [...new Set(all.map((e) => e.node))].sort();
  const types = [...new Set(all.map((e) => e.type))].sort();
  const shown = all
    .filter((e) => (!node || e.node === node) && (!type || e.type === type))
    .filter((e) => !search || [e.trace, e.task_id].some((v) => String(v ?? "").includes(search)))
    .sort((a, b) => lamport ? a.lamport - b.lamport || a.node.localeCompare(b.node) : a.wall_ms - b.wall_ms)
    .slice(-400);
  return (
    <Card title="Event log" action={
      <div className="flex flex-wrap gap-2 text-sm">
        <input aria-label="trace or task id" placeholder="trace / task id" value={search}
          onChange={(e) => setSearch(e.target.value)} className="h-8 rounded border border-rule bg-panel px-2" />
        <select aria-label="node" className="h-8 rounded border border-rule bg-panel px-2" value={node} onChange={(e) => setNode(e.target.value)}>
          <option value="">all nodes</option>{nodes.map((n) => <option key={n}>{n}</option>)}
        </select>
        <select aria-label="event type" className="h-8 rounded border border-rule bg-panel px-2" value={type} onChange={(e) => setType(e.target.value)}>
          <option value="">all types</option>{types.map((t) => <option key={t}>{t}</option>)}
        </select>
        <label className="flex items-center gap-1"><input type="checkbox" checked={lamport} onChange={(e) => setLamport(e.target.checked)} />Lamport order</label>
      </div>}>
      {shown.length === 0 ? <PanelState kind="empty" message="Waiting for events from the live stream" /> : (
        <div className="max-h-[640px] overflow-auto font-mono text-xs" data-testid="event-log">
          {shown.map((e, i) => (
            <div key={i} className="grid grid-cols-[70px_110px_120px_190px_1fr] gap-2 border-b border-rule py-0.5">
              <span className="text-muted">L{e.lamport}</span>
              <span className="text-muted">{new Date(e.wall_ms).toLocaleTimeString([], { hour12: false })}</span>
              <span>{e.node}</span>
              <span className="font-semibold">{e.type}</span>
              <span className="truncate">{e.task_id} {e.trace ? `trace=${e.trace}` : ""}</span>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}
