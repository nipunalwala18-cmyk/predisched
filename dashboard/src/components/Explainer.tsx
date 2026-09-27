import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { Candidate } from "../api/types";

/** A stored decision's breakdown JSON (snake_case, prompt 18) as candidates. */
export function fromBreakdown(json: string | null | undefined): Candidate[] {
  if (!json) return [];
  try {
    return (JSON.parse(json) as Record<string, any>[]).map((c) => ({
      workerId: String(c.worker),
      chosen: Boolean(c.chosen),
      score: c.score ?? null,
      predQueueLen: c.pred_queue_len ?? null,
      predictedWaitMs: c.predicted_wait_ms ?? null,
      predictedExecMs: c.predicted_exec_ms ?? null,
      overloadProb: c.overload_prob ?? null,
      overloadPenaltyMs: c.overload_penalty_ms ?? null,
      cost: c.cost ?? null,
      skipped: Boolean(c.skipped),
      skipReason: String(c.skip_reason ?? ""),
      coldStart: Boolean(c.cold_start),
    }));
  } catch {
    return [];
  }
}

/**
 * Why this worker (F13, spec §15.4): per candidate, predicted wait + predicted execution +
 * overload penalty = cost, the winner highlighted. Reactive decisions show their own score.
 */
export function Explainer({ candidates, strategy, fallbackReason }: {
  candidates: Candidate[]; strategy?: string; fallbackReason?: string;
}) {
  if (!candidates.length) {
    return <p className="text-sm text-muted">No per-worker breakdown for this decision.</p>;
  }
  const predicted = candidates.some((c) => c.predictedExecMs !== null && Number.isFinite(c.predictedExecMs));
  const data = candidates.map((c) => ({
    name: `${c.chosen ? "★ " : ""}${c.workerId}`,
    wait: c.predictedWaitMs ?? 0,
    exec: c.predictedExecMs ?? 0,
    penalty: c.overloadPenaltyMs ?? 0,
    score: c.score ?? 0,
    chosen: c.chosen,
  }));
  return (
    <div data-testid="explainer">
      {fallbackReason && (
        <p className="mb-2 text-sm" style={{ color: "var(--warn)" }}>Fallback: {fallbackReason}</p>
      )}
      <div className="h-48 w-full">
        <ResponsiveContainer>
          <BarChart data={data} layout="vertical" margin={{ left: 8, right: 16 }}>
            <CartesianGrid horizontal={false} />
            <XAxis type="number" fontSize={11} unit={predicted ? " ms" : ""} />
            <YAxis type="category" dataKey="name" width={96} fontSize={12} />
            <Tooltip contentStyle={{ background: "var(--panel)", border: "1px solid var(--rule)" }} />
            <Legend />
            {predicted ? (
              <>
                <Bar dataKey="wait" name="predicted wait" stackId="c" fill="#2a78d6" isAnimationActive={false} />
                <Bar dataKey="exec" name="predicted execution" stackId="c" fill="#1baf7a" isAnimationActive={false} />
                <Bar dataKey="penalty" name="overload penalty" stackId="c" fill="#eb6834" isAnimationActive={false} />
              </>
            ) : (
              <Bar dataKey="score" name={`${strategy ?? "strategy"} score (lower wins)`} fill="#2a78d6"
                isAnimationActive={false} />
            )}
          </BarChart>
        </ResponsiveContainer>
      </div>
      <table className="tabular mt-2 w-full text-xs">
        <thead className="text-muted">
          <tr><th className="text-left">Worker</th><th>Queue</th><th>Wait</th><th>Exec</th><th>Overload</th><th>Cost</th><th className="text-left">Note</th></tr>
        </thead>
        <tbody>
          {candidates.map((c) => (
            <tr key={c.workerId} className={c.chosen ? "font-semibold" : ""}>
              <td>{c.chosen ? "★ " : ""}{c.workerId}</td>
              <td className="text-center">{num(c.predQueueLen, 2)}</td>
              <td className="text-center">{num(c.predictedWaitMs)}</td>
              <td className="text-center">{num(c.predictedExecMs)}</td>
              <td className="text-center">{num(c.overloadProb, 2)}</td>
              <td className="text-center">{num(c.cost ?? c.score)}</td>
              <td>{c.skipped ? `skipped: ${c.skipReason}` : c.coldStart ? "cold start" : c.chosen ? "chosen" : ""}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const num = (v: number | null, d = 1) => (v === null || !Number.isFinite(v) ? "–" : v.toFixed(d));
