import { useQuery } from "@tanstack/react-query";
import { getJson } from "./client";
import type {
  LeaderResponse,
  Overview,
  Row,
  TaskDetail,
  WorkersResponse,
} from "./types";

// Polling reads (TanStack Query); the live stream fills in between (stream/store.ts).
export const useOverview = () =>
  useQuery({ queryKey: ["overview"], queryFn: () => getJson<Overview>("/api/overview"), refetchInterval: 2000 });

export const useWorkers = () =>
  useQuery({ queryKey: ["workers"], queryFn: () => getJson<WorkersResponse>("/api/workers"), refetchInterval: 2000 });

export const useWorkerHistory = (id: string, minutes: number) =>
  useQuery({
    queryKey: ["worker-history", id, minutes],
    queryFn: () => getJson<{ samples: Row[] }>(`/api/workers/${encodeURIComponent(id)}/history?minutes=${minutes}`),
    refetchInterval: 5000,
  });

export const useTasks = (status: string, type: string) =>
  useQuery({
    queryKey: ["tasks", status, type],
    queryFn: () => {
      const q = new URLSearchParams({ limit: "1000" });
      if (status) q.set("status", status);
      if (type) q.set("type", type);
      return getJson<{ tasks: Row[] }>(`/api/tasks?${q}`);
    },
    refetchInterval: 3000,
  });

export const useTask = (id: string | null) =>
  useQuery({
    queryKey: ["task", id],
    queryFn: () => getJson<TaskDetail>(`/api/tasks/${encodeURIComponent(id ?? "")}`),
    enabled: !!id,
  });

export const useQueueForecast = (minutes: number) =>
  useQuery({
    queryKey: ["forecast", minutes],
    queryFn: () => getJson<{ actual: Row[]; predicted: Row[]; horizonSeconds: number }>(
      `/api/queue/forecast?minutes=${minutes}`),
    refetchInterval: 3000,
  });

export const useAccuracy = () =>
  useQuery({
    queryKey: ["accuracy"],
    queryFn: () => getJson<{ recent: Row[]; byType: Row[]; rollingMaeMs: number | null }>(
      "/api/predictions/accuracy?limit=1000"),
    refetchInterval: 5000,
  });

export const useModels = () =>
  useQuery({ queryKey: ["models"], queryFn: () => getJson<{ models: Record<string, Row>; drift: Row[]; error?: string }>("/api/models"), refetchInterval: 15000 });

export const useBenchmarks = () =>
  useQuery({ queryKey: ["benchmarks"], queryFn: () => getJson<{ suites: Row[]; runs: Row[]; reports: string[] }>("/api/benchmarks?limit=2000"), refetchInterval: 30000 });

export const useLeader = () =>
  useQuery({ queryKey: ["leader"], queryFn: () => getJson<LeaderResponse>("/api/cluster/leader"), refetchInterval: 2000 });

export const useReplication = () =>
  useQuery({ queryKey: ["replication"], queryFn: () => getJson<{ headSeq: number; replicas: Row[] }>("/api/replication/status"), refetchInterval: 5000 });

export const useClocks = () =>
  useQuery({ queryKey: ["clocks"], queryFn: () => getJson<{ nodes: Row[] }>("/api/clock/offsets"), refetchInterval: 10000 });
