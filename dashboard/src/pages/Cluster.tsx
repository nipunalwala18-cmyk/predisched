import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useClocks, useReplication, useTasks, useWorkers } from "../api/hooks";
import { sinceMs, useApp } from "../AppContext";
import { ClockOffsets, ElectionTimeline, Gantt, LoadHeatmap } from "../components/ClusterPanels";
import { Card, Guard, PanelState } from "../components/ui/primitives";
import { WorkerCard } from "../components/WorkerCard";
import { history } from "../history";
import { stream, useStreamVersion } from "../stream/store";

export function ClusterPage() {
  useStreamVersion();
  const { range } = useApp();
  const since = sinceMs(range);
  const workers = useWorkers();
  const tasks = useTasks("", "");
  const clocks = useClocks();
  const replication = useReplication();
  const metrics = stream.metrics.filter((m) => m.t >= since);
  const latest = metrics[metrics.length - 1];
  return (
    <div className="space-y-4">
      <Card title="Workers">
        <Guard query={workers} empty={(w) => !w.workers.length}>
          {(w) => (
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
              {w.workers.map((row) => <WorkerCard key={String(row.worker_id)} stored={row} live={row.live} />)}
            </div>
          )}
        </Guard>
      </Card>
      <div className="grid gap-4 xl:grid-cols-2">
        <Card title="Load heatmap (busy threads / pool)">
          {metrics.length ? <LoadHeatmap metrics={metrics} /> : <PanelState kind="empty" message="Waiting for the live stream" />}
        </Card>
        <Card title="Thread pools: busy, idle, queued">
          {latest ? (
            <div className="h-56" data-testid="thread-pools">
              <ResponsiveContainer>
                <BarChart data={latest.workers.map((w) => ({ worker: w.workerId, busy: w.activeThreads,
                  idle: Math.max(0, w.poolSize - w.activeThreads), queued: w.queueLen }))}>
                  <CartesianGrid vertical={false} />
                  <XAxis dataKey="worker" fontSize={11} />
                  <YAxis allowDecimals={false} fontSize={11} width={28} />
                  <Tooltip contentStyle={{ background: "var(--panel)", border: "1px solid var(--rule)" }} />
                  <Legend />
                  <Bar dataKey="busy" stackId="p" fill="#2a78d6" isAnimationActive={false} />
                  <Bar dataKey="idle" stackId="p" fill="var(--rule)" isAnimationActive={false} />
                  <Bar dataKey="queued" stackId="p" fill="#eb6834" isAnimationActive={false} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : <PanelState kind="empty" message="Waiting for the live stream" />}
        </Card>
      </div>
      <Card title="Timeline: tasks per worker">
        <Guard query={tasks} empty={(t) => !t.tasks.some((x) => x.started_at)}>
          {(t) => <Gantt tasks={t.tasks.slice(0, 400)} />}
        </Guard>
      </Card>
      <div className="grid gap-4 xl:grid-cols-3">
        <Card title="Election timeline: who leads">
          {history.leaders.length ? <ElectionTimeline history={history.leaders.filter((h) => h.t >= since)}
            markers={stream.markers} /> : <PanelState kind="loading" />}
        </Card>
        <Card title="Clock sync: offsets before and after">
          <Guard query={clocks} empty={(c) => !c.nodes.length}>{(c) => <ClockOffsets nodes={c.nodes} />}</Guard>
        </Card>
        <Card title="Replication">
          <Guard query={replication} empty={(r) => !r.replicas.length}>
            {(r) => (
              <table className="tabular w-full text-sm" data-testid="replication">
                <thead className="text-xs text-muted"><tr><th className="text-left">Node</th><th>Last seq</th><th>Lag</th></tr></thead>
                <tbody>
                  {r.replicas.map((x) => (
                    <tr key={String(x.node_id)} className="border-b border-rule">
                      <td>{x.node_id}</td><td className="text-center">{x.max_seq}</td>
                      <td className="text-center" style={{ color: x.lag > 0 ? "var(--warn)" : undefined }}>{x.lag}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </Guard>
        </Card>
      </div>
    </div>
  );
}
