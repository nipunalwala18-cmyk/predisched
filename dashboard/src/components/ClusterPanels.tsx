import * as d3 from "d3";
import { useEffect, useRef } from "react";
import {
  Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip,
  XAxis, YAxis,
} from "recharts";
import type { Row } from "../api/types";
import type { Marker, MetricsPoint } from "../stream/store";
import { workerColor } from "../theme/palette";
import { markerLines, timeTick } from "./ui/charts";

/** Who led, over time (spec §15.3): a step line from polls, elections as markers. */
export function ElectionTimeline({ history, markers }: { history: { t: number; leader: number | null }[]; markers: Marker[] }) {
  const data = history.map((h) => ({ t: h.t, leader: h.leader ?? 0 }));
  const lo = data.length ? data[0].t : 0;
  return (
    <div className="h-48 w-full" data-testid="election-timeline">
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 16, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid vertical={false} />
          <XAxis dataKey="t" type="number" scale="time" domain={["dataMin", "dataMax"]} tickFormatter={timeTick} fontSize={11} />
          <YAxis allowDecimals={false} domain={[0, 5]} fontSize={11} width={28}
            label={{ value: "leader", angle: -90, position: "insideLeft", fill: "var(--muted)", fontSize: 11 }} />
          <Tooltip labelFormatter={(t) => timeTick(Number(t))}
            contentStyle={{ background: "var(--panel)", border: "1px solid var(--rule)" }} />
          <Line type="stepAfter" dataKey="leader" stroke="var(--warn)" strokeWidth={3} dot={false} isAnimationActive={false} />
          {markerLines(markers.filter((m) => m.kind === "election" || m.kind === "chaos").filter((m) => m.t >= lo))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

/** Worker × time heatmap of busy threads over pool size (D3): imbalance shows as banding. */
export function LoadHeatmap({ metrics }: { metrics: MetricsPoint[] }) {
  const ref = useRef<SVGSVGElement>(null);
  useEffect(() => {
    const svg = d3.select(ref.current);
    svg.selectAll("*").remove();
    if (!metrics.length) return;
    const width = ref.current?.clientWidth ?? 600;
    const workers = [...new Set(metrics.flatMap((m) => m.workers.map((w) => w.workerId)))].sort();
    const height = Math.max(60, workers.length * 26 + 24);
    svg.attr("height", height);
    const x = d3.scaleTime().domain(d3.extent(metrics, (m) => m.t) as [number, number]).range([90, width - 10]);
    const y = d3.scaleBand().domain(workers).range([0, height - 24]).padding(0.08);
    const color = d3.scaleSequential(d3.interpolateYlOrRd).domain([0, 1]);
    const cellW = Math.max(2, (width - 100) / metrics.length);
    for (const m of metrics) {
      for (const w of m.workers) {
        svg.append("rect").attr("x", x(m.t)).attr("y", y(w.workerId) ?? 0).attr("width", cellW)
          .attr("height", y.bandwidth()).attr("fill", color(w.poolSize ? w.activeThreads / w.poolSize : 0))
          .append("title").text(`${w.workerId} ${timeTick(m.t)}: ${w.activeThreads}/${w.poolSize} busy, queue ${w.queueLen}`);
      }
    }
    svg.append("g").attr("transform", "translate(88,0)").call(d3.axisLeft(y).tickSize(0))
      .call((g) => g.select(".domain").remove()).selectAll("text").attr("fill", "var(--muted)");
    svg.append("g").attr("transform", `translate(0,${height - 22})`)
      .call(d3.axisBottom(x).ticks(6).tickFormat((d) => timeTick(Number(d))))
      .selectAll("text").attr("fill", "var(--muted)");
  }, [metrics]);
  return <svg ref={ref} className="w-full" role="img" aria-label="worker load heatmap" data-testid="heatmap" />;
}

/** One swimlane per worker, every task a bar (D3 Gantt, spec §15.3). */
export function Gantt({ tasks }: { tasks: Row[] }) {
  const ref = useRef<SVGSVGElement>(null);
  useEffect(() => {
    const svg = d3.select(ref.current);
    svg.selectAll("*").remove();
    const rows = tasks.filter((t) => t.started_at && t.completed_at && t.worker_id);
    if (!rows.length) return;
    const width = ref.current?.clientWidth ?? 600;
    const workers = [...new Set(rows.map((t) => String(t.worker_id)))].sort();
    const height = workers.length * 30 + 26;
    svg.attr("height", height);
    const start = (t: Row) => new Date(t.started_at).getTime();
    const end = (t: Row) => new Date(t.completed_at).getTime();
    const x = d3.scaleTime().domain([d3.min(rows, start)!, d3.max(rows, end)!]).range([90, width - 10]);
    const y = d3.scaleBand().domain(workers).range([0, height - 24]).padding(0.15);
    for (const t of rows) {
      svg.append("rect").attr("x", x(start(t))).attr("y", y(String(t.worker_id)) ?? 0)
        .attr("width", Math.max(1.5, x(end(t)) - x(start(t)))).attr("height", y.bandwidth())
        .attr("rx", 2).attr("fill", workerColor(String(t.worker_id)))
        .attr("opacity", t.status === "COMPLETED" ? 0.8 : 0.35)
        .append("title").text(`${t.task_id} ${t.type} ${t.exec_time_ms ?? "?"} ms`);
    }
    svg.append("g").attr("transform", "translate(88,0)").call(d3.axisLeft(y).tickSize(0))
      .call((g) => g.select(".domain").remove()).selectAll("text").attr("fill", "var(--muted)");
    svg.append("g").attr("transform", `translate(0,${height - 22})`)
      .call(d3.axisBottom(x).ticks(6).tickFormat((d) => timeTick(Number(d))))
      .selectAll("text").attr("fill", "var(--muted)");
  }, [tasks]);
  return <svg ref={ref} className="w-full" role="img" aria-label="task timeline" data-testid="gantt" />;
}

/** Each node's clock offset before and after the last sync round (diverging bars, Exp 3). */
export function ClockOffsets({ nodes }: { nodes: Row[] }) {
  const data = nodes.map((n) => ({ node: String(n.node_id), before: Number(n.offset_before_ms), after: Number(n.offset_after_ms) }));
  return (
    <div className="h-56 w-full" data-testid="clock-offsets">
      <ResponsiveContainer>
        <BarChart data={data} layout="vertical" margin={{ left: 10, right: 16 }}>
          <CartesianGrid horizontal={false} />
          <XAxis type="number" unit=" ms" fontSize={11} />
          <YAxis type="category" dataKey="node" width={90} fontSize={11} />
          <Tooltip contentStyle={{ background: "var(--panel)", border: "1px solid var(--rule)" }} />
          <Legend />
          <ReferenceLine x={0} stroke="var(--muted)" />
          <Bar dataKey="before" name="offset before" fill="#eb6834" isAnimationActive={false} />
          <Bar dataKey="after" name="offset after" fill="#2a78d6" isAnimationActive={false} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
