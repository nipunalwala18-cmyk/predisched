# Prompt 17 — Dashboard UI (React + TypeScript)

**Spec sections:** §4 (FR17), §13 (React views)
**Depends on:** 16
**Lab topics:** none directly (makes every topic visible in a live demo)
**Commit message:** `Dashboard: live cluster, tasks, predictions, events and benchmark views`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Build the operator dashboard in `dashboard/` with Vite,
React 18, TypeScript (strict), React Router, TanStack Query for REST, `@stomp/stompjs` for live
updates, and Recharts. No UI component library beyond that; write small components and one CSS file
of design tokens (light and dark).

### Views (spec §13)

1. **Cluster overview** — one card per scheduler (role badge, strategy, consistency, clock offset) and
   per worker (CPU, memory, active threads / pool size, queue, status, backend). The leader is
   visually marked. Dead workers stay visible, greyed, with time since last heartbeat. Updates live.
2. **Tasks** — filterable table (status, type, run id), live row updates, detail drawer with the task's
   Lamport-ordered event timeline, worker, wait and exec time, predicted vs actual.
3. **Predictions** — predicted vs actual exec time scatter, rolling MAE line, per-worker overload
   probability bars, fallback rate.
4. **Queue** — actual vs forecast queue length over time per worker.
5. **Events** — Lamport-ordered log with filters for elections, failovers, clock sync and failures;
   elections and failovers highlighted.
6. **Benchmarks** — pick a suite and scenario: latency CDF, throughput with CI whiskers, imbalance,
   SLA %, and the significance table from Prompt 15's summary.
7. **Control** (only when the API allows writes) — submit a task, switch strategy, kill a node (fault
   injection), each with a confirmation step.

### Engineering requirements

- Typed API client generated from the API's OpenAPI document (`openapi-typescript`), not hand-written
  types.
- One WebSocket connection shared app-wide, with reconnect and a visible "live / reconnecting" status.
- Charts bound to bounded windows (e.g. last 5 minutes) so memory stays flat during long demos.
- Loading, empty and error states on every view. Keyboard accessible; colour is never the only signal
  of status.
- Works at 1280 px and down to 390 px width.

### Tests

- Vitest + Testing Library: worker card renders dead state; task filters update the query; the events
  view orders by (Lamport, node).
- One Playwright smoke test against the API running with fixture data: every route renders without
  console errors.

### Acceptance checks

```bash
cd dashboard && npm ci && npm run lint && npm test && npm run build
npm run dev   # with the API and a cluster running; kill the leader and watch the badge move
```

### Docs

`docs/components/dashboard.md` with a screenshot of each view in `docs/components/img/`.
