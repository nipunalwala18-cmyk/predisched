import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Overview } from "../api/types";
import { Explainer, fromBreakdown } from "../components/Explainer";
import { KpiStrip } from "../components/KpiStrip";
import { WorkerCard } from "../components/WorkerCard";

// Fixture API responses in the shapes the dashboard API returns (docs/components/dashboard-api.md).
const overview: Overview = {
  generatedAt: "2026-09-27T10:49:52Z",
  kpis: { windowSeconds: 60, tasksPerSecond: 2, meanLatencyMs: 875.7, p95LatencyMs: 1603.3,
    slaCompliancePct: 90, queueDepth: 7, activeWorkers: 3 },
  cluster: null,
};

describe("KpiStrip", () => {
  it("renders the six numbers from an overview", () => {
    render(<KpiStrip overview={overview} history={[]} />);
    expect(screen.getByText("Tasks / s")).toBeInTheDocument();
    expect(screen.getByText("2.00")).toBeInTheDocument();
    expect(screen.getByText("876 ms")).toBeInTheDocument();
    expect(screen.getByText("1603 ms")).toBeInTheDocument();
    expect(screen.getByText("90.0 %")).toBeInTheDocument();
    expect(screen.getByText("7")).toBeInTheDocument();
  });

  it("shows a dash for numbers the API does not have", () => {
    render(<KpiStrip overview={{ ...overview, kpis: { ...overview.kpis, slaCompliancePct: null, queueDepth: null } }} history={[]} />);
    expect(screen.getAllByText("–").length).toBeGreaterThanOrEqual(2);
  });
});

describe("WorkerCard", () => {
  it("renders CPU and memory rings, threads and status", () => {
    render(<WorkerCard stored={{ worker_id: "worker-2", host: "localhost", port: 51062, cores: 8,
      cpu_pct: 42.4, mem_pct: 7.1, pool_size: 4, tasks_completed: 120 }}
      live={{ workerId: "worker-2", host: "localhost", port: 51062, poolSize: 4, healthy: true,
        draining: false, activeThreads: 3, queueLen: 2 }} />);
    expect(screen.getByText("worker-2")).toBeInTheDocument();
    expect(screen.getByLabelText("CPU 42%")).toBeInTheDocument();
    expect(screen.getByLabelText("Memory 7%")).toBeInTheDocument();
    expect(screen.getByText("3/4")).toBeInTheDocument();
    expect(screen.getByText("HEALTHY")).toBeInTheDocument();
  });

  it("marks a worker without a live entry as dead", () => {
    render(<WorkerCard stored={{ worker_id: "worker-9", cpu_pct: 0, mem_pct: 0 }} live={null} />);
    expect(screen.getByText("DEAD")).toBeInTheDocument();
  });
});

describe("Explainer", () => {
  const breakdown = JSON.stringify([
    { worker: "worker-1", score: 172.0, chosen: false, pred_queue_len: 0.22, predicted_wait_ms: 26.1,
      predicted_exec_ms: 71.2, overload_prob: 0.768, overload_penalty_ms: 74.7, cost: 172.0,
      skipped: false, skip_reason: "", cold_start: false },
    { worker: "worker-2", score: 29.1, chosen: true, pred_queue_len: 0, predicted_wait_ms: 0,
      predicted_exec_ms: 29.1, overload_prob: 0, overload_penalty_ms: 0, cost: 29.1,
      skipped: false, skip_reason: "", cold_start: false },
    { worker: "worker-3", score: 44.0, chosen: false, pred_queue_len: 0, predicted_wait_ms: 0,
      predicted_exec_ms: 44.0, overload_prob: 0.9, overload_penalty_ms: 39.6, cost: 83.6,
      skipped: true, skip_reason: "overload 0.90 > 0.80", cold_start: false },
  ]);

  it("parses a stored decision and highlights the winner", () => {
    const candidates = fromBreakdown(breakdown);
    expect(candidates).toHaveLength(3);
    render(<Explainer candidates={candidates} strategy="predictive" />);
    expect(screen.getByTestId("explainer")).toBeInTheDocument();
    expect(screen.getByText("★ worker-2")).toBeInTheDocument();
    expect(screen.getByText("chosen")).toBeInTheDocument();
    expect(screen.getByText("skipped: overload 0.90 > 0.80")).toBeInTheDocument();
    expect(screen.getByText("172.0")).toBeInTheDocument();
  });

  it("says so when a fallback placed the task", () => {
    render(<Explainer candidates={fromBreakdown(breakdown)} fallbackReason="least_loaded: circuit breaker open" />);
    expect(screen.getByText("Fallback: least_loaded: circuit breaker open")).toBeInTheDocument();
  });

  it("copes with a missing breakdown", () => {
    expect(fromBreakdown(null)).toEqual([]);
    render(<Explainer candidates={[]} />);
    expect(screen.getByText("No per-worker breakdown for this decision.")).toBeInTheDocument();
  });
});
