import { Background, ReactFlow, type Edge, type Node } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { ClusterSnapshot } from "../api/types";
import { workerColor } from "../theme/palette";

/**
 * Client → leader → workers (spec §15.2). The leader wears a crown; followers and unhealthy
 * workers fade. Edges animate while tasks are running.
 */
export function Topology({ cluster }: { cluster: ClusterSnapshot }) {
  const nodes: Node[] = [];
  const edges: Edge[] = [];
  const busy = cluster.running > 0;
  nodes.push({ id: "client", position: { x: 0, y: 120 }, data: { label: "client" },
    style: { background: "var(--panel)", color: "var(--ink)", border: "1px solid var(--rule)" } });
  const schedulers = cluster.nodes.length
    ? cluster.nodes
    : [{ id: cluster.leaderId ?? 1, address: cluster.answeredBy, leader: true, reachable: true }];
  schedulers.forEach((s, i) => {
    const id = `s${s.id}`;
    nodes.push({
      id,
      position: { x: 220, y: i * 60 + (schedulers.length === 1 ? 110 : 0) },
      data: { label: `${s.leader ? "👑 " : ""}scheduler ${s.id}` },
      style: {
        background: s.leader ? "var(--accent)" : "var(--panel)",
        color: s.leader ? "#fff" : "var(--ink)",
        border: "1px solid var(--rule)",
        opacity: s.reachable ? (s.leader ? 1 : 0.6) : 0.25,
        fontWeight: s.leader ? 600 : 400,
      },
    });
    if (s.leader) edges.push({ id: `c-${id}`, source: "client", target: id, animated: busy });
  });
  const leader = schedulers.find((s) => s.leader);
  cluster.workers.forEach((w, i) => {
    const id = `w-${w.workerId}`;
    nodes.push({
      id,
      position: { x: 470, y: i * 70 + 40 },
      data: { label: `${w.workerId} · ${w.activeThreads}/${w.poolSize}${w.draining ? " · draining" : ""}` },
      style: { background: "var(--panel)", color: "var(--ink)", border: `2px solid ${workerColor(w.workerId)}`,
        opacity: w.healthy ? 1 : 0.3 },
    });
    if (leader) edges.push({ id: `l-${id}`, source: `s${leader.id}`, target: id,
      animated: w.activeThreads > 0, style: { stroke: workerColor(w.workerId) } });
  });
  return (
    <div className="h-72 w-full" data-testid="topology">
      <ReactFlow nodes={nodes} edges={edges} fitView nodesDraggable={false} nodesConnectable={false}
        zoomOnScroll={false} panOnDrag={false} proOptions={{ hideAttribution: true }}>
        <Background gap={24} color="var(--grid)" />
      </ReactFlow>
    </div>
  );
}
