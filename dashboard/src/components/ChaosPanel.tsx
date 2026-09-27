import { useQueryClient } from "@tanstack/react-query";
import { Activity, Cpu, Flame, Gauge, Scissors, Skull, Zap } from "lucide-react";
import { useState, type ReactNode } from "react";
import { postJson } from "../api/client";
import type { WorkerLive } from "../api/types";
import { Button, Dialog } from "./ui/primitives";

interface Action {
  id: string;
  title: string;
  icon: ReactNode;
  effect: string;
  needsNode: boolean;
  body: (node: string) => Record<string, unknown>;
}

const ACTIONS: Action[] = [
  { id: "kill-worker", title: "Kill worker", icon: <Skull size={16} />, needsNode: true,
    effect: "Crashes the worker process; its running tasks are re-queued after missed heartbeats.",
    body: (node) => ({ node }) },
  { id: "kill-primary", title: "Kill primary", icon: <Flame size={16} />, needsNode: false,
    effect: "Crashes the primary scheduler; an election and a backup promotion follow.",
    body: () => ({}) },
  { id: "latency", title: "Inject latency", icon: <Gauge size={16} />, needsNode: true,
    effect: "Adds 1,500 ms to every call the node receives, for 60 s.",
    body: (node) => ({ node, ms: 1500, seconds: 60 }) },
  { id: "cpu", title: "Spike CPU", icon: <Cpu size={16} />, needsNode: true,
    effect: "Busy-loops one thread per core on the node for 30 s, so overload prediction fires.",
    body: (node) => ({ node, seconds: 30 }) },
  { id: "partition", title: "Partition node", icon: <Scissors size={16} />, needsNode: true,
    effect: "Refuses the node's replication traffic both ways for 20 s.",
    body: (node) => ({ node, seconds: 20 }) },
  { id: "drain", title: "Drain worker", icon: <Activity size={16} />, needsNode: true,
    effect: "The worker finishes what it has and accepts nothing new.",
    body: (node) => ({ node }) },
  { id: "burst", title: "Submit burst", icon: <Zap size={16} />, needsNode: false,
    effect: "Submits 200 bursty tasks at once.",
    body: () => ({ tasks: 200, profile: "bursty" }) },
];

/** Chaos controls with confirmation dialogs (spec §15.7); each action becomes a chart marker. */
export function ChaosPanel({ workers, schedulers }: { workers: WorkerLive[]; schedulers: number[] }) {
  const [pending, setPending] = useState<Action | null>(null);
  const [node, setNode] = useState("");
  const [result, setResult] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const client = useQueryClient();
  const nodes = [...workers.map((w) => w.workerId), ...schedulers.map((s) => `scheduler-${s}`)];

  async function run(action: Action) {
    setBusy(true);
    try {
      const r = await postJson<Record<string, unknown>>(`/api/chaos/${action.id}`, action.body(node), true);
      setResult(`${action.title}: ${String(r.message ?? (r.ok ? "done" : "refused"))}`);
      client.invalidateQueries();
    } catch (e) {
      setResult(`${action.title} failed: ${(e as Error).message}`);
    } finally {
      setBusy(false);
      setPending(null);
    }
  }

  return (
    <div data-testid="chaos-panel">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {ACTIONS.map((a) => (
          <Button key={a.id} variant={a.id.startsWith("kill") ? "danger" : "outline"} size="lg"
            onClick={() => { setNode(a.needsNode ? (nodes[0] ?? "") : ""); setPending(a); }}>
            {a.icon}{a.title}
          </Button>
        ))}
      </div>
      {result && <p className="mt-3 text-sm text-ink" role="status" data-testid="chaos-result">{result}</p>}
      <Dialog open={!!pending} onOpenChange={(o) => !o && setPending(null)} title={`${pending?.title ?? ""}?`}>
        {pending && (
          <div className="space-y-4 text-sm">
            <p>{pending.effect}</p>
            {pending.needsNode && (
              <label className="block">
                <span className="text-muted">Node</span>
                <select className="mt-1 w-full rounded border border-rule bg-panel p-2 text-ink"
                  value={node} onChange={(e) => setNode(e.target.value)}>
                  {nodes.map((n) => <option key={n} value={n}>{n}</option>)}
                </select>
              </label>
            )}
            <div className="flex justify-end gap-2">
              <Button variant="outline" onClick={() => setPending(null)}>Cancel</Button>
              <Button variant="danger" disabled={busy || (pending.needsNode && !node)} onClick={() => run(pending)}>
                Confirm
              </Button>
            </div>
          </div>
        )}
      </Dialog>
    </div>
  );
}
