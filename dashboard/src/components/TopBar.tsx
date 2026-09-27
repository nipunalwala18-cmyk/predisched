import { useQueryClient } from "@tanstack/react-query";
import { Pause, Play, PlayCircle } from "lucide-react";
import { NavLink } from "react-router-dom";
import { postJson } from "../api/client";
import type { Overview } from "../api/types";
import { useApp, type Range } from "../AppContext";
import { THEMES, type Theme } from "../theme/palette";
import { Button } from "./ui/primitives";

export const PAGES = [
  { path: "/", label: "Overview" },
  { path: "/cluster", label: "Cluster" },
  { path: "/tasks", label: "Tasks" },
  { path: "/predictions", label: "Predictions" },
  { path: "/benchmarks", label: "Benchmarks" },
  { path: "/chaos", label: "Chaos" },
  { path: "/logs", label: "Logs" },
];

const STATUS: Record<string, [string, string]> = {
  live: ["var(--good)", "LIVE"],
  connecting: ["var(--muted)", "CONNECTING"],
  reconnecting: ["var(--warn)", "RECONNECTING"],
  offline: ["var(--bad)", "OFFLINE"],
};

/** The persistent top bar (spec §15.1): connection, leader, strategy, model, pause control. */
export function TopBar({ overview, onDemo }: { overview?: Overview; onDemo: () => void }) {
  const { status, range, setRange, theme, setTheme } = useApp();
  const client = useQueryClient();
  const c = overview?.cluster;
  const [dot, label] = STATUS[status] ?? STATUS.offline;
  const togglePause = async () => {
    await postJson("/api/admin/pause", { paused: !c?.paused }, true);
    client.invalidateQueries({ queryKey: ["overview"] });
  };
  return (
    <header className="sticky top-0 z-30 border-b border-rule bg-panel">
      <div className="flex flex-wrap items-center gap-x-5 gap-y-2 px-4 py-2 text-sm">
        <span className="text-base font-bold tracking-tight">PrediSched</span>
        <span className="flex items-center gap-1.5" data-testid="connection" title="live stream">
          <span className="h-2.5 w-2.5 rounded-full" style={{ background: dot }} />
          <span className="font-medium">{label}</span>
        </span>
        <span>Leader: <b data-testid="leader">{c?.leaderId ? `S${c.leaderId}` : c ? c.answeredBy : "–"}</b></span>
        <span>Strategy: <b data-testid="strategy">{c?.strategy?.toUpperCase() ?? "–"}</b></span>
        <span>Model: <b>{c?.modelVersions ? c.modelVersions.split(",")[0].replace("m1=", "M1 ") : "–"}</b></span>
        {!c && overview?.clusterError && <span style={{ color: "var(--bad)" }}>cluster: {overview.clusterError}</span>}
        <div className="ml-auto flex items-center gap-2">
          <select aria-label="time range" className="h-8 rounded border border-rule bg-panel px-2"
            value={range} onChange={(e) => setRange(e.target.value as Range)}>
            {["1m", "5m", "15m", "all"].map((r) => <option key={r} value={r}>{r === "all" ? "whole run" : `last ${r}`}</option>)}
          </select>
          <select aria-label="theme" className="h-8 rounded border border-rule bg-panel px-2"
            value={theme} onChange={(e) => setTheme(e.target.value as Theme)}>
            {THEMES.map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
          <Button variant="outline" size="sm" onClick={onDemo} title="demo mode (d)">
            <PlayCircle size={16} />Demo
          </Button>
          <Button variant={c?.paused ? "default" : "danger"} size="sm" onClick={togglePause} disabled={!c}
            title="pause or resume the queue (p)" data-testid="pause">
            {c?.paused ? <><Play size={16} />RESUME QUEUE</> : <><Pause size={16} />PAUSE QUEUE</>}
          </Button>
        </div>
      </div>
      <nav className="flex gap-1 overflow-x-auto px-3" aria-label="pages">
        {PAGES.map((p, i) => (
          <NavLink key={p.path} to={p.path} end
            className={({ isActive }) => `border-b-2 px-3 py-2 text-sm ${isActive ? "border-accent font-semibold text-ink" : "border-transparent text-muted hover:text-ink"}`}
            title={`${p.label} (${i + 1})`}>
            {p.label}
          </NavLink>
        ))}
      </nav>
    </header>
  );
}
