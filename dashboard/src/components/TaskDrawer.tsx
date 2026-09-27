import { useTask } from "../api/hooks";
import type { Candidate } from "../api/types";
import { Explainer, fromBreakdown } from "./Explainer";
import { Dialog, Guard } from "./ui/primitives";

/** Slide-over task detail (spec §15.4): lifecycle, Lamport times, retries, the decision explainer. */
export function TaskDrawer({ taskId, onClose }: { taskId: string | null; onClose: () => void }) {
  const query = useTask(taskId);
  return (
    <Dialog open={!!taskId} onOpenChange={(open) => !open && onClose()} title={`Task ${taskId ?? ""}`} side>
      <Guard query={query}>
        {(d) => {
          const last = d.decisions[d.decisions.length - 1];
          const live = d.explanation?.found ? d.explanation : null;
          const candidates: Candidate[] = live?.candidates?.length
            ? live.candidates : fromBreakdown(last?.breakdown);
          return (
            <div className="space-y-5 text-sm">
              <dl className="tabular grid grid-cols-2 gap-x-4 gap-y-1">
                {["type", "input", "status", "priority", "worker_id", "attempts", "exec_time_ms",
                  "strategy", "trace_id", "result"].map((k) => (
                  <div key={k} className="contents">
                    <dt className="text-muted">{k}</dt>
                    <dd className="truncate">{String(d.task[k] ?? "–")}</dd>
                  </div>
                ))}
              </dl>
              <section>
                <h3 className="mb-2 font-semibold">Why {String(last?.chosen_worker ?? d.task.worker_id ?? "this worker")}?</h3>
                {last || live ? (
                  <Explainer candidates={candidates} strategy={String(last?.strategy ?? live?.strategy ?? "")}
                    fallbackReason={last?.fallback ? String(last.fallback_reason) : live?.fallback ? live.fallbackReason : undefined} />
                ) : (
                  <p className="text-muted">No decision recorded for this task.</p>
                )}
              </section>
              <section>
                <h3 className="mb-2 font-semibold">Lifecycle (Lamport order)</h3>
                <ol className="space-y-1">
                  {d.lifecycle.map((e, i) => (
                    <li key={i} className="tabular flex gap-3 text-xs">
                      <span className="w-12 text-muted">L{e.lamport_time}</span>
                      <span className="w-24 text-muted">{new Date(e.ts).toLocaleTimeString([], { hour12: false })}</span>
                      <span className="w-40 font-medium">{e.event_type}</span>
                      <span className="text-muted">{e.node_id}</span>
                    </li>
                  ))}
                </ol>
              </section>
              {d.predictions.length > 0 && (
                <section>
                  <h3 className="mb-2 font-semibold">Prediction vs actual</h3>
                  {d.predictions.map((p, i) => (
                    <p key={i} className="tabular text-xs">
                      {p.worker_id}: predicted {Number(p.pred_exec_ms).toFixed(1)} ms, actual {p.actual_exec_ms} ms
                      (error {Number(p.abs_error_ms).toFixed(1)} ms{p.cold_start ? ", cold start" : ""})
                    </p>
                  ))}
                </section>
              )}
            </div>
          );
        }}
      </Guard>
    </Dialog>
  );
}
