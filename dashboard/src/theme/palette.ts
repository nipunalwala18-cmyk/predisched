// One colour per worker, per task status and per strategy, everywhere in the app (spec §15.11).
// Categorical slots of the reference palette in fixed order; identity follows the entity.
export const SERIES = [
  "#2a78d6", // blue
  "#eb6834", // orange
  "#1baf7a", // aqua
  "#eda100", // yellow
  "#e87ba4", // magenta
  "#008300", // green
  "#4a3aa7", // violet
  "#e34948", // red
];

export const STRATEGY_COLORS: Record<string, string> = {
  round_robin: SERIES[0],
  random: SERIES[1],
  least_loaded: SERIES[2],
  resource_aware: SERIES[3],
  predictive: SERIES[4],
  "scale:none": SERIES[0],
  "scale:reactive": SERIES[1],
  "scale:predictive": SERIES[4],
};

export const STRATEGY_LABELS: Record<string, string> = {
  round_robin: "Round robin",
  random: "Random",
  least_loaded: "Least loaded",
  resource_aware: "Resource-aware",
  predictive: "Predictive",
};

// Status is a reserved role (never a series colour) and always ships with a label.
export const STATUS_COLORS: Record<string, string> = {
  COMPLETED: "var(--good)",
  RUNNING: "var(--accent)",
  QUEUED: "var(--muted)",
  BLOCKED: "var(--warn)",
  FAILED: "var(--bad)",
  CANCELLED: "var(--muted)",
};

const workerOrder: string[] = [];

/** A stable colour per worker id: the first worker seen gets slot 1, and so on. */
export function workerColor(id: string): string {
  let i = workerOrder.indexOf(id);
  if (i < 0) {
    workerOrder.push(id);
    workerOrder.sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
    i = workerOrder.indexOf(id);
  }
  return SERIES[i % SERIES.length];
}

export type Theme = "light" | "dark" | "projector";
export const THEMES: Theme[] = ["light", "dark", "projector"];
