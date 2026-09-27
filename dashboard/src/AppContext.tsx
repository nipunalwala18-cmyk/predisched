import { createContext, useContext } from "react";
import type { Theme } from "./theme/palette";
import type { StreamStatus } from "./stream/useStomp";

export type Range = "1m" | "5m" | "15m" | "all";
export const RANGE_MINUTES: Record<Range, number> = { "1m": 1, "5m": 5, "15m": 15, all: 60 };

export interface AppState {
  range: Range;
  setRange: (r: Range) => void;
  theme: Theme;
  setTheme: (t: Theme) => void;
  status: StreamStatus;
  reconnects: number;
}

export const AppContext = createContext<AppState>({
  range: "5m",
  setRange: () => {},
  theme: "light",
  setTheme: () => {},
  status: "connecting",
  reconnects: 0,
});

export const useApp = () => useContext(AppContext);

/** The cut-off time for the selected range (the stream buffer holds five minutes). */
export function sinceMs(range: Range, now = Date.now()): number {
  return now - RANGE_MINUTES[range] * 60_000;
}
