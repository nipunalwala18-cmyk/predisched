import type { Row, WorkerLive } from "../api/types";
import { workerColor } from "../theme/palette";
import { Badge } from "./ui/primitives";
import { Ring } from "./ui/charts";

/** One worker: CPU and memory rings, threads, queue, status (spec §15.3). */
export function WorkerCard({ stored, live }: { stored: Row; live: WorkerLive | null }) {
  const id = String(stored.worker_id ?? live?.workerId);
  const color = workerColor(id);
  const status = !live ? "DEAD" : live.draining ? "DRAINING" : live.healthy ? "HEALTHY" : "STALE";
  const statusColor = { HEALTHY: "var(--good)", DRAINING: "var(--warn)", STALE: "var(--warn)",
    DEAD: "var(--bad)" }[status];
  const pool = live?.poolSize ?? stored.pool_size ?? 0;
  const active = live?.activeThreads ?? stored.active_threads ?? 0;
  return (
    <article className="rounded-lg border border-rule bg-panel p-4" data-testid="worker-card"
      style={{ borderTop: `3px solid ${color}` }}>
      <header className="mb-3 flex items-center justify-between">
        <div>
          <div className="font-semibold text-ink">{id}</div>
          <div className="text-xs text-muted">{stored.host ?? live?.host}:{stored.port ?? live?.port}
            {stored.cores ? ` · ${stored.cores} cores` : ""}</div>
        </div>
        <Badge color={statusColor}>{status}</Badge>
      </header>
      <div className="flex items-center justify-around">
        <Ring value={Number(stored.cpu_pct ?? 0)} label="CPU" color={color} />
        <Ring value={Number(stored.mem_pct ?? 0)} label="Memory" color={color} />
      </div>
      <dl className="tabular mt-3 grid grid-cols-3 gap-2 text-center text-xs">
        <div><dt className="text-muted">Threads</dt><dd className="text-sm text-ink">{active}/{pool}</dd></div>
        <div><dt className="text-muted">Queue</dt><dd className="text-sm text-ink">{live?.queueLen ?? stored.queue_len ?? 0}</dd></div>
        <div><dt className="text-muted">Done</dt><dd className="text-sm text-ink">{stored.tasks_completed ?? "–"}</dd></div>
      </dl>
      <div className="mt-2 h-1.5 w-full overflow-hidden rounded bg-surface" aria-label="thread pool">
        <div className="h-full" style={{ width: `${pool ? (100 * active) / pool : 0}%`, background: color }} />
      </div>
    </article>
  );
}
