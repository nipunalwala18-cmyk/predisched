# Prompt 22: Dashboard API with admin controls (spec §15.10, F7)

## Goal

A Spring Boot service exposes every REST endpoint and the WebSocket stream the dashboard needs, backed
by PostgreSQL and live gRPC calls to the cluster, including admin controls to switch strategy, drain a
worker, pause the queue and trigger an election.

## Read first

- Spec §15.10, §15.9, §4 FR17, FR31, §6 Tier 1 (F7), §8 (Dashboard API, Live updates rows)

## Build

1. **Protos**: `proto/admin.proto` with `AdminService` on the scheduler: `SetStrategy`,
   `PauseQueue`, `ResumeQueue`, `DrainWorker`, `TriggerElection`, `GetClusterState` (leader, nodes,
   workers, queue depth, strategy, model versions). Only the primary accepts mutating calls.
2. **New module `predisched-dashboard-api`** (Spring Boot 3.3.x, depends on `predisched-common`):
   - Every endpoint in spec §15.10, with the same paths. Reads come from PostgreSQL (plain
     `JdbcTemplate`, no JPA needed); live state comes from `GetClusterState` on the primary, found
     through the scheduler list in config.
   - Admin and chaos endpoints map to `AdminService` and `ChaosService`. They require an admin API key
     header and are rate limited.
   - `/api/benchmarks/run` starts `run-suite` as a background job and returns a run id; progress goes
     to the WebSocket.
   - `/api/simulate` calls the what-if simulator (prompt 20) in-process.
   - `/api/models` reads `registry.json`; `/api/models/{v}/promote` runs the promotion rule from
     prompt 21 (returns 409 if that prompt is not done).
   - `WS /ws/stream` (STOMP): topics `/topic/metrics`, `/topic/tasks`, `/topic/events`,
     `/topic/decisions`. The API subscribes to the cluster (tail of the event log or a
     `StreamEvents` server-streaming RPC; add it to `admin.proto`) and throttles to about 4 messages
     per second per topic, batching within each tick.
   - OpenAPI docs via springdoc at `/swagger-ui`. Actuator health at `/actuator/health`.
   - CORS allows the Vite dev server origin `http://localhost:5173`.
3. **REST task submission (F24, small)**: `POST /api/tasks` forwards to `SubmitTask` with the caller's
   API key.

## Tests

- `@WebMvcTest` per controller with mocked gRPC clients and repositories: shapes, status codes,
  admin key enforcement.
- Integration with Testcontainers Postgres and an in-process scheduler: `/api/overview` returns live
  numbers; `POST /api/admin/strategy` changes the strategy the scheduler reports.
- WebSocket: a test client receives throttled batches (≤ 5 messages per second per topic).

## Acceptance checks

```bash
java -jar predisched-dashboard-api/target/predisched-dashboard-api.jar
```
```bash
curl -s localhost:8080/api/overview
```
```bash
curl -s -X POST -H "X-Admin-Key: dev-admin" -H "Content-Type: application/json" -d "{\"strategy\":\"least_loaded\"}" localhost:8080/api/admin/strategy
```

Paste the overview JSON, the strategy switch response, and `curl localhost:8080/api/cluster/leader`
before and after `POST /api/chaos/kill-primary`.

## Docs and commit

- `docs/components/dashboard-api.md`: endpoint table, WebSocket topics, auth.
- Commit: `Dashboard API: REST, WebSocket stream and admin controls for the cluster`
