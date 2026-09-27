// Shapes of the dashboard API's answers (docs/components/dashboard-api.md).

export interface WorkerLive {
  workerId: string;
  host: string;
  port: number;
  poolSize: number;
  healthy: boolean;
  draining: boolean;
  activeThreads: number;
  queueLen: number;
}

export interface NodeState {
  id: number;
  address: string;
  leader: boolean;
  reachable: boolean;
}

export interface ClusterSnapshot {
  answeredBy: string;
  leaderId: number | null;
  strategy: string;
  modelVersions: string | null;
  paused: boolean;
  queueDepth: number;
  running: number;
  arrivalRatePerSec: number;
  decisionP50Ms: number;
  decisionP95Ms: number;
  liveMaeMs: number | null;
  speculations: number;
  autoscale: string;
  tasksTotal: number;
  tasksCompleted: number;
  tasksFailed: number;
  nodes: NodeState[];
  workers: WorkerLive[];
  atMs: number;
}

export interface Kpis {
  windowSeconds: number;
  tasksPerSecond: number;
  meanLatencyMs: number | null;
  p95LatencyMs: number | null;
  slaCompliancePct: number | null;
  queueDepth: number | null;
  activeWorkers: number | null;
}

export interface Overview {
  generatedAt: string;
  kpis: Kpis;
  cluster: ClusterSnapshot | null;
  clusterError?: string;
}

export type Row = Record<string, any>;

export interface WorkersResponse {
  workers: (Row & { live: WorkerLive | null })[];
  clusterError: string | null;
}

export interface Candidate {
  workerId: string;
  chosen: boolean;
  score: number | null;
  predQueueLen: number | null;
  predictedWaitMs: number | null;
  predictedExecMs: number | null;
  overloadProb: number | null;
  overloadPenaltyMs: number | null;
  cost: number | null;
  skipped: boolean;
  skipReason: string;
  coldStart: boolean;
}

export interface Explanation {
  found: boolean;
  message?: string;
  strategy?: string;
  chosenWorker?: string;
  fallback?: boolean;
  fallbackReason?: string;
  decisionUs?: number;
  modelVersions?: string;
  candidates?: Candidate[];
}

export interface TaskDetail {
  task: Row;
  lifecycle: Row[];
  decisions: Row[];
  predictions: Row[];
  explanation: Explanation | null;
}

export interface LeaderResponse {
  leader: number | null;
  leaderAddress: string | null;
  nodes: Row[];
  elections: Row[];
}

export interface StreamEvent {
  node: string;
  lamport: number;
  physical_ms: number;
  wall_ms: number;
  type: string;
  task_id: string;
  trace: string;
  [key: string]: unknown;
}
