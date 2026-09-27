import { memo } from "react";
import {
  CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Scatter, ScatterChart,
  Tooltip, XAxis, YAxis,
} from "recharts";
import type { Row } from "../api/types";
import type { Marker } from "../stream/store";
import { SERIES } from "../theme/palette";
import { markerLines, timeTick } from "./ui/charts";

/**
 * Each executor's resource profile (the worker's *TaskExecutor.profile()). Thirteen task types are
 * more than the palette's eight colours can tell apart, so the scatter colours by these five and
 * names the task type in the tooltip.
 */
const PROFILE_OF: Record<string, string> = {
  CPU_TASK: "CPU bound", HASH_TASK: "CPU bound", MONTE_CARLO_TASK: "CPU bound", ML_INFER_TASK: "CPU bound",
  COMPRESS_TASK: "memory bound", GRAPH_TASK: "memory bound", SORT_TASK: "memory bound",
  SLEEP_TASK: "I/O bound", FILE_IO_TASK: "I/O bound", DB_QUERY_TASK: "I/O bound",
  HTTP_TASK: "network bound",
  MATRIX_TASK: "parallel", MAPREDUCE_TASK: "parallel",
};
const PROFILES = ["CPU bound", "memory bound", "I/O bound", "network bound", "parallel"];

function PointTip({ active, payload }: { active?: boolean; payload?: { payload?: Record<string, number | string> }[] }) {
  const p = active ? payload?.[0]?.payload : undefined;
  if (!p) return null;
  return (
    <div className="rounded border border-rule bg-panel px-2 py-1 text-xs tabular">
      <div className="font-semibold">{p.type}</div>
      <div>actual {Number(p.actual).toFixed(0)} ms · predicted {Number(p.predicted).toFixed(0)} ms</div>
    </div>
  );
}

/** Every completed task, predicted vs actual on log axes, with the y = x line (spec §15.5). */
function PredVsActualChart({ rows }: { rows: Row[] }) {
  const max = Math.max(10, ...rows.map((r) => Math.max(Number(r.pred_exec_ms), Number(r.actual_exec_ms))));
  const point = (r: Row) => ({ type: String(r.task_type).replace("_TASK", ""),
    actual: Math.max(1, Number(r.actual_exec_ms)), predicted: Math.max(1, Number(r.pred_exec_ms)) });
  return (
    <div className="h-80 w-full" data-testid="pred-scatter">
      <ResponsiveContainer>
        <ScatterChart margin={{ top: 10, right: 20, bottom: 10, left: 0 }}>
          <CartesianGrid />
          <XAxis type="number" dataKey="actual" name="actual" unit=" ms" scale="log" domain={[1, max]}
            allowDataOverflow fontSize={11} />
          <YAxis type="number" dataKey="predicted" name="predicted" unit=" ms" scale="log" domain={[1, max]}
            allowDataOverflow fontSize={11} width={60} />
          <Tooltip cursor={{ strokeDasharray: "3 3" }} content={PointTip} />
          <Legend />
          <ReferenceLine segment={[{ x: 1, y: 1 }, { x: max, y: max }]} stroke="var(--muted)"
            strokeDasharray="5 4" ifOverflow="hidden" label={{ value: "y = x", fill: "var(--muted)", fontSize: 11 }} />
          {PROFILES.map((p, i) => (
            <Scatter key={p} name={p} fill={SERIES[i]} fillOpacity={0.7} isAnimationActive={false}
              data={rows.filter((r) => (PROFILE_OF[String(r.task_type)] ?? "CPU bound") === p).map(point)} />
          ))}
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
}

/** Rolling MAE over time with the drift threshold (1.5 Ã— test MAE) drawn in. */
function MaeOverTimeChart({ rows, testMae, markers }: { rows: Row[]; testMae: number | null; markers: Marker[] }) {
  const data = [...rows].reverse().map((r) => ({ t: new Date(r.ts).getTime(), mae: Number(r.rolling_mae_ms) }));
  const lo = data.length ? data[0].t : 0;
  return (
    <div className="h-64 w-full" data-testid="mae-chart">
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 16, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid vertical={false} />
          <XAxis dataKey="t" type="number" scale="time" domain={["dataMin", "dataMax"]} tickFormatter={timeTick} fontSize={11} />
          <YAxis unit=" ms" fontSize={11} width={60} />
          <Tooltip labelFormatter={(t) => timeTick(Number(t))}
            contentStyle={{ background: "var(--panel)", border: "1px solid var(--rule)" }} />
          <Line type="monotone" dataKey="mae" name="rolling MAE" stroke="var(--accent)" strokeWidth={2}
            dot={false} isAnimationActive={false} />
          {testMae ? (
            <ReferenceLine y={1.5 * testMae} stroke="var(--warn)" strokeDasharray="6 4"
              label={{ value: `drift threshold ${(1.5 * testMae).toFixed(0)} ms`, fill: "var(--muted)", fontSize: 11, position: "insideTopRight" }} />
          ) : null}
          {markerLines(markers.filter((m) => m.t >= lo))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export const PredVsActual = memo(PredVsActualChart);

export const MaeOverTime = memo(MaeOverTimeChart);
