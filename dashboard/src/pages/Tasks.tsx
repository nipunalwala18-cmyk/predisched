import { useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useTasks } from "../api/hooks";
import type { Row } from "../api/types";
import { TaskDrawer } from "../components/TaskDrawer";
import { TaskTable } from "../components/TaskTable";
import { Card, Guard, PanelState } from "../components/ui/primitives";
import { STATUS_COLORS } from "../theme/palette";

const STATUSES = ["", "QUEUED", "RUNNING", "COMPLETED", "FAILED", "CANCELLED", "BLOCKED"];
const TYPES = ["", "CPU_TASK", "MATRIX_TASK", "SLEEP_TASK", "SORT_TASK", "HASH_TASK", "COMPRESS_TASK",
  "MONTE_CARLO_TASK", "FILE_IO_TASK", "HTTP_TASK", "GRAPH_TASK", "DB_QUERY_TASK", "MAPREDUCE_TASK", "ML_INFER_TASK"];

function histogram(tasks: Row[]) {
  const edges = [0, 50, 100, 200, 500, 1000, 2000, 5000, Infinity];
  return edges.slice(0, -1).map((lo, i) => ({
    bucket: edges[i + 1] === Infinity ? `≥${lo}` : `${lo}-${edges[i + 1]}`,
    n: tasks.filter((t) => t.latency_ms != null && t.latency_ms >= lo && t.latency_ms < edges[i + 1]).length,
  }));
}

export function TasksPage() {
  const [status, setStatus] = useState("");
  const [type, setType] = useState("");
  const [open, setOpen] = useState<string | null>(null);
  const tasks = useTasks(status, type);
  const all = useTasks("", "");
  return (
    <div className="space-y-4">
      <Card title="Tasks" action={
        <div className="flex gap-2 text-sm">
          <select aria-label="status filter" className="h-8 rounded border border-rule bg-panel px-2" value={status}
            onChange={(e) => setStatus(e.target.value)}>
            {STATUSES.map((s) => <option key={s} value={s}>{s || "any status"}</option>)}
          </select>
          <select aria-label="type filter" className="h-8 rounded border border-rule bg-panel px-2" value={type}
            onChange={(e) => setType(e.target.value)}>
            {TYPES.map((s) => <option key={s} value={s}>{s || "any type"}</option>)}
          </select>
        </div>}>
        <Guard query={tasks} empty={(t) => !t.tasks.length}>
          {(t) => <TaskTable tasks={t.tasks} onOpen={setOpen} />}
        </Guard>
      </Card>
      <div className="grid gap-4 xl:grid-cols-3">
        <Card title="Status funnel">
          <Guard query={all} empty={(t) => !t.tasks.length}>
            {(t) => {
              const counts = STATUSES.slice(1).map((s) => ({ status: s, n: t.tasks.filter((x) => x.status === s).length }));
              return (
                <div className="h-56" data-testid="funnel">
                  <ResponsiveContainer>
                    <BarChart data={counts} layout="vertical">
                      <CartesianGrid horizontal={false} />
                      <XAxis type="number" allowDecimals={false} fontSize={11} />
                      <YAxis type="category" dataKey="status" width={90} fontSize={11} />
                      <Tooltip contentStyle={{ background: "var(--panel)", border: "1px solid var(--rule)" }} />
                      <Bar dataKey="n" isAnimationActive={false}>
                        {counts.map((c) => <Cell key={c.status} fill={STATUS_COLORS[c.status]} />)}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              );
            }}
          </Guard>
        </Card>
        <Card title="Latency distribution (ms)">
          <Guard query={all} empty={(t) => !t.tasks.some((x) => x.latency_ms != null)}>
            {(t) => (
              <div className="h-56" data-testid="latency-histogram">
                <ResponsiveContainer>
                  <BarChart data={histogram(t.tasks)}>
                    <CartesianGrid vertical={false} />
                    <XAxis dataKey="bucket" fontSize={10} />
                    <YAxis allowDecimals={false} fontSize={11} width={32} />
                    <Tooltip contentStyle={{ background: "var(--panel)", border: "1px solid var(--rule)" }} />
                    <Bar dataKey="n" name="tasks" fill="#2a78d6" radius={[4, 4, 0, 0]} isAnimationActive={false} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
          </Guard>
        </Card>
        <Card title="Workflow DAG · dead-letter queue">
          <PanelState kind="notbuilt" message="Not built yet: the API has no workflow or dead-letter endpoint (use predisched workflow status / dlq list)" />
        </Card>
      </div>
      <TaskDrawer taskId={open} onClose={() => setOpen(null)} />
    </div>
  );
}
