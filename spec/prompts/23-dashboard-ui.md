# Prompt 23: React dashboard (spec §15)

## Goal

A React + TypeScript dashboard where, within ten seconds of loading, a viewer can see which node is
the leader, how loaded each worker is, what the scheduler predicts, and whether predictions are
coming true. All seven pages exist, and the seven priority panels are polished.

## Read first

- Spec §15 (all of it), especially §15.12 build priority and §15.11 implementation notes

## Build

Build in the §15.12 order and commit working increments inside this prompt if it helps, but the final
commit is one.

1. **Scaffold** `dashboard/`: Vite + React + TypeScript, Tailwind + shadcn/ui, TanStack Query,
   Recharts, D3 (heatmap, Gantt), React Flow (topology, DAG), a STOMP WebSocket hook with automatic
   reconnect and a connection indicator. API base URL from `VITE_API_URL`.
2. **Shell**: the persistent top bar (live status, leader, strategy, model version, pause/resume queue
   button) and the tabs Overview, Cluster, Tasks, Predictions, Benchmarks, Chaos, Logs.
3. **Priority panels** (§15.12), each wired to real endpoints:
   1. KPI strip with sparklines;
   2. worker cards with CPU/memory rings;
   3. live queue chart, actual vs predicted;
   4. virtualised task table with filters and the task detail drawer;
   5. predicted vs actual scatter with the y = x line;
   6. strategy comparison grouped bars from the latest benchmark suite;
   7. chaos buttons with confirmation dialogs.
4. **Remaining widgets** from §15.2–§15.8 as time allows, in page order. The decision explainer in the
   task drawer (F13) and the election timeline come first. Leave a visible "not built yet" empty state
   for any widget skipped, and list the skipped ones in the component doc.
5. **Cross-cutting** (§15.9, §15.11): time-range selector, a bounded 5-minute client buffer, updates
   throttled to about 4 per second, one colour per worker / status / strategy from a shared palette,
   dark mode and a high-contrast projector theme, empty and error states on every panel, keyboard
   shortcuts for page switching and pausing.
6. **Chaos markers**: every chaos and election event draws a vertical marker on the time-series
   charts.
7. **Demo mode** (§15.9): a scripted sequence (submit workload → kill primary → spike a worker →
   burst) with on-screen captions, driven by the real API.

## Tests

- Vitest + React Testing Library: the KPI strip, worker card and explainer render from fixture API
  responses; the WebSocket hook reconnects; the throttle caps update rate.
- Playwright smoke test against a running API: every page loads without console errors.
- CI: add a Node 20 job running `npm ci`, `npm run lint`, `npm test`, `npm run build`.

## Acceptance checks

```bash
npm --prefix dashboard run build
```
```bash
npm --prefix dashboard run dev
```

With the cluster, API and a replay running, capture screenshots of Overview, Cluster, Tasks (drawer
open on an explained decision), Predictions and Benchmarks into `docs/img/dashboard-*.png`, then kill
the primary from the Chaos page and capture the topology and election timeline updating.

## Docs and commit

- `docs/components/dashboard.md`: pages, data sources per widget, screenshots, skipped widgets.
- Commit: `Dashboard: live cluster, task, prediction and benchmark views with chaos controls`
