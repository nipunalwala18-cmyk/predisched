import { useVirtualizer } from "@tanstack/react-virtual";
import { useRef } from "react";
import type { Row } from "../api/types";
import { STATUS_COLORS, workerColor } from "../theme/palette";

const COLUMNS = "grid grid-cols-[1.6fr_1.2fr_0.5fr_0.9fr_0.9fr_0.8fr_0.8fr_0.5fr_1fr] gap-2";

/** Every task, virtualised so thousands of rows scroll smoothly (spec §15.4). */
export function TaskTable({ tasks, onOpen }: { tasks: Row[]; onOpen: (id: string) => void }) {
  const parent = useRef<HTMLDivElement>(null);
  const rows = useVirtualizer({
    count: tasks.length,
    getScrollElement: () => parent.current,
    estimateSize: () => 34,
    overscan: 12,
  });
  return (
    <div className="text-sm" data-testid="task-table">
      <div className={`${COLUMNS} border-b border-rule px-2 py-2 text-xs font-medium text-muted`}>
        <span>Task</span><span>Type</span><span>Prio</span><span>Status</span><span>Worker</span>
        <span className="text-right">Latency</span><span className="text-right">Exec</span>
        <span className="text-right">Tries</span><span>Strategy</span>
      </div>
      <div ref={parent} className="h-[520px] overflow-auto">
        <div style={{ height: rows.getTotalSize(), position: "relative" }}>
          {rows.getVirtualItems().map((v) => {
            const t = tasks[v.index];
            return (
              <button key={t.task_id} onClick={() => onOpen(String(t.task_id))}
                className={`${COLUMNS} absolute left-0 w-full items-center border-b border-rule px-2 text-left hover:bg-surface`}
                style={{ height: v.size, transform: `translateY(${v.start}px)` }}>
                <span className="truncate font-mono text-xs">{t.task_id}</span>
                <span className="truncate">{String(t.type).replace("_TASK", "")}</span>
                <span className="tabular">{t.priority}</span>
                <span className="flex items-center gap-1.5">
                  <span className="h-2 w-2 rounded-full" style={{ background: STATUS_COLORS[t.status] ?? "var(--muted)" }} />
                  {t.status}
                </span>
                <span className="flex items-center gap-1.5 truncate">
                  {t.worker_id && <span className="h-2 w-2 rounded-full" style={{ background: workerColor(String(t.worker_id)) }} />}
                  {t.worker_id ?? "–"}
                </span>
                <span className="tabular text-right">{t.latency_ms == null ? "–" : `${Number(t.latency_ms).toFixed(0)} ms`}</span>
                <span className="tabular text-right">{t.exec_time_ms == null ? "–" : `${t.exec_time_ms} ms`}</span>
                <span className="tabular text-right">{t.attempts}</span>
                <span className="truncate text-muted">{t.strategy ?? ""}</span>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}
