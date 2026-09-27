import { useCallback, useEffect, useState } from "react";
import { Route, Routes, useNavigate } from "react-router-dom";
import { postJson, WS_URL } from "./api/client";
import { useOverview } from "./api/hooks";
import { AppContext, type Range } from "./AppContext";
import { DemoMode } from "./components/DemoMode";
import { PAGES, TopBar } from "./components/TopBar";
import { history } from "./history";
import { BenchmarksPage } from "./pages/Benchmarks";
import { ChaosPage } from "./pages/Chaos";
import { ClusterPage } from "./pages/Cluster";
import { LogsPage } from "./pages/Logs";
import { OverviewPage } from "./pages/Overview";
import { PredictionsPage } from "./pages/Predictions";
import { TasksPage } from "./pages/Tasks";
import { stream } from "./stream/store";
import { useStomp } from "./stream/useStomp";
import { THEMES, type Theme } from "./theme/palette";

const TOPICS = ["metrics", "tasks", "events", "decisions"];

function storedTheme(): Theme {
  try {
    const t = localStorage.getItem("predisched.theme") as Theme | null;
    if (t && THEMES.includes(t)) return t;
  } catch {
    // no storage
  }
  return window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

export function App() {
  const [range, setRange] = useState<Range>("5m");
  const [theme, setThemeState] = useState<Theme>(storedTheme);
  const [demo, setDemo] = useState(false);
  const navigate = useNavigate();
  const overview = useOverview();
  const ingest = useCallback((topic: string, items: unknown[]) => stream.ingest(topic, items), []);
  const { status, reconnects } = useStomp(WS_URL, TOPICS, ingest);

  const setTheme = (t: Theme) => {
    setThemeState(t);
    try { localStorage.setItem("predisched.theme", t); } catch { /* ignore */ }
  };
  useEffect(() => { document.documentElement.dataset.theme = theme; }, [theme]);
  useEffect(() => {
    if (!overview.data) return;
    history.record(overview.data);
    stream.noteLeader(overview.data.cluster?.leaderId);
  }, [overview.data]);

  // Keyboard shortcuts: 1-7 pages, p pause/resume, t theme, d demo (spec §15.9).
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement;
      if (["INPUT", "SELECT", "TEXTAREA"].includes(target.tagName) || e.ctrlKey || e.metaKey || e.altKey) return;
      const n = Number(e.key);
      if (n >= 1 && n <= PAGES.length) navigate(PAGES[n - 1].path);
      else if (e.key === "p" && overview.data?.cluster) {
        postJson("/api/admin/pause", { paused: !overview.data.cluster.paused }, true).then(() => overview.refetch());
      } else if (e.key === "t") setTheme(THEMES[(THEMES.indexOf(theme) + 1) % THEMES.length]);
      else if (e.key === "d") setDemo(true);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });

  return (
    <AppContext.Provider value={{ range, setRange, theme, setTheme, status, reconnects }}>
      <div className="min-h-full bg-surface text-ink">
        <TopBar overview={overview.data} onDemo={() => setDemo(true)} />
        <main className="mx-auto max-w-[1600px] p-4">
          <Routes>
            <Route path="/" element={<OverviewPage />} />
            <Route path="/cluster" element={<ClusterPage />} />
            <Route path="/tasks" element={<TasksPage />} />
            <Route path="/predictions" element={<PredictionsPage />} />
            <Route path="/benchmarks" element={<BenchmarksPage />} />
            <Route path="/chaos" element={<ChaosPage />} />
            <Route path="/logs" element={<LogsPage />} />
          </Routes>
        </main>
        <DemoMode open={demo} onClose={() => setDemo(false)}
          firstWorker={overview.data?.cluster?.workers[0]?.workerId ?? "worker-1"} />
      </div>
    </AppContext.Provider>
  );
}
