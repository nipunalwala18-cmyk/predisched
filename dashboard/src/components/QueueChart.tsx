import { memo } from "react";
import { Area, CartesianGrid, ComposedChart, Legend, Line, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { Row } from "../api/types";
import type { Marker } from "../stream/store";
import { markerLines, timeTick } from "./ui/charts";

/**
 * Actual queued tasks vs the M2 forecast (spec §15.2): the forecast line runs ahead of the
 * actual one by its 5 s horizon. Chaos and election markers are drawn across it.
 */
function QueueChartChart({ actual, predicted, markers }: {
  actual: Row[]; predicted: Row[]; markers: Marker[];
}) {
  const byT = new Map<number, { t: number; actual?: number; predicted?: number }>();
  for (const r of actual) {
    const t = new Date(r.ts).getTime();
    byT.set(t, { ...(byT.get(t) ?? { t }), actual: Number(r.queue_len) });
  }
  for (const r of predicted) {
    const t = new Date(r.ts).getTime();
    byT.set(t, { ...(byT.get(t) ?? { t }), predicted: Number(r.queue_len) });
  }
  const data = [...byT.values()].sort((a, b) => a.t - b.t);
  const lo = data.length ? data[0].t : 0;
  return (
    <div className="h-64 w-full" data-testid="queue-chart">
      <ResponsiveContainer>
        <ComposedChart data={data} margin={{ top: 16, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid vertical={false} />
          <XAxis dataKey="t" type="number" scale="time" domain={["dataMin", "dataMax"]}
            tickFormatter={timeTick} fontSize={11} />
          <YAxis allowDecimals={false} fontSize={11} width={36} />
          <Tooltip labelFormatter={(t) => timeTick(Number(t))}
            contentStyle={{ background: "var(--panel)", border: "1px solid var(--rule)" }} />
          <Legend />
          <Area type="monotone" dataKey="actual" name="actual queue" stroke="var(--accent)"
            fill="var(--accent)" fillOpacity={0.15} strokeWidth={2} isAnimationActive={false} connectNulls />
          <Line type="monotone" dataKey="predicted" name="forecast (+5 s)" stroke="#eb6834"
            strokeWidth={2} strokeDasharray="5 4" dot={false} isAnimationActive={false} connectNulls />
          {markerLines(markers.filter((m) => m.t >= lo))}
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}

export const QueueChart = memo(QueueChartChart);
