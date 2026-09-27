import { Line, LineChart, ReferenceLine, ResponsiveContainer, YAxis } from "recharts";
import type { Marker } from "../../stream/store";

/** A radial ring (CPU, memory): the arc shows the share, the centre the number. */
export function Ring({ value, label, color, size = 64 }: {
  value: number; label: string; color: string; size?: number;
}) {
  const v = Math.max(0, Math.min(100, Number.isFinite(value) ? value : 0));
  const r = size / 2 - 5;
  const c = 2 * Math.PI * r;
  return (
    <figure className="flex flex-col items-center gap-1" aria-label={`${label} ${v.toFixed(0)}%`}>
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
        <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="var(--rule)" strokeWidth={6} />
        <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke={color} strokeWidth={6}
          strokeLinecap="round" strokeDasharray={`${(v / 100) * c} ${c}`}
          transform={`rotate(-90 ${size / 2} ${size / 2})`} />
        <text x="50%" y="52%" textAnchor="middle" dominantBaseline="middle"
          className="tabular" fontSize={size / 4.5} fill="var(--ink)">{v.toFixed(0)}%</text>
      </svg>
      <figcaption className="text-xs text-muted">{label}</figcaption>
    </figure>
  );
}

/** A small trend line for the KPI strip. */
export function Sparkline({ data, color = "var(--accent)" }: { data: number[]; color?: string }) {
  if (data.length < 2) return <div className="h-8" />;
  const points = data.map((v, i) => ({ i, v }));
  return (
    <div className="h-8 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={points} margin={{ top: 4, bottom: 4, left: 0, right: 0 }}>
          <YAxis hide domain={["auto", "auto"]} />
          <Line type="monotone" dataKey="v" stroke={color} strokeWidth={2} dot={false}
            isAnimationActive={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

const MARKER_COLORS: Record<Marker["kind"], string> = {
  chaos: "var(--bad)",
  election: "var(--warn)",
  failure: "var(--bad)",
  admin: "var(--muted)",
  scale: "var(--good)",
  drift: "var(--warn)",
};

/** Vertical chaos/election markers for a time-series chart with an x axis in ms. */
export function markerLines(markers: Marker[]) {
  // A fault and the election it causes land seconds apart: step each label down a line so
  // neighbouring labels do not print over each other.
  return markers.map((m, i) => (
    <ReferenceLine key={`${m.t}-${i}`} x={m.t} stroke={MARKER_COLORS[m.kind]}
      strokeDasharray="4 3" ifOverflow="extendDomain"
      label={{ value: m.label, position: "insideTopLeft", fill: "var(--muted)", fontSize: 10, dy: (i % 3) * 12 }} />
  ));
}

export const timeTick = (t: number) => new Date(t).toLocaleTimeString([], { hour12: false });
