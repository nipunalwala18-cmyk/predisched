# Prompt 16 — Dashboard API (Spring Boot)

**Spec sections:** §4 (FR17), §13 (endpoints), §6 (Dashboard API row)
**Depends on:** 15
**Lab topics:** none directly (exposes every component for observation)
**Commit message:** `Dashboard API: REST and WebSocket views of cluster, tasks, predictions, events and benchmarks`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Add `predisched-dashboard-api`, a Spring Boot 3 service
that gives the UI a read-mostly view of the running product.

### Build

- New Maven module `predisched-dashboard-api` (Spring Boot 3.3.x managed through the parent's
  `dependencyManagement` via the Spring Boot BOM import — do not make Spring Boot the parent POM of
  the whole project). Port 8080.
- Data comes from two places: **PostgreSQL** (history) via Spring Data JDBC or `JdbcTemplate`, and the
  **live cluster** via the existing gRPC `AdminService` on the current leader (reuse `SchedulerClient`
  leader discovery from Prompt 05).
- REST (spec §13), all paginated where lists can grow, all returning DTOs (never JPA entities or
  protobuf messages):
  - `GET /api/scheduler` — leader id, each scheduler's role, strategy, consistency mode, election
    algorithm, clock offsets.
  - `GET /api/workers` — capacity, latest metrics, status, backend.
  - `GET /api/tasks?status&type&runId&page&size` and `GET /api/tasks/{id}` (with its events).
  - `GET /api/metrics?workerId&from&to&step` — downsampled time series.
  - `GET /api/predictions?from&to&workerId` — predicted vs actual, rolling MAE, fallback rate.
  - `GET /api/events?from&to&type` — ordered by (Lamport, node).
  - `GET /api/benchmarks` and `GET /api/benchmarks/{suite}` — summaries from `benchmark_runs`.
  - `POST /api/tasks` (submit) and `POST /api/admin/strategy` — the only writes, both forwarded to the
    leader over gRPC; disabled unless `dashboard.allowWrites=true`.
- WebSocket (STOMP over `/ws`): topics `/topic/workers` (1 s snapshots), `/topic/tasks` (state changes),
  `/topic/events`, `/topic/leader`. A single `LiveFeed` component polls the leader's admin stream and
  fans out; it survives leader changes.
- Actuator `health` including DB and leader reachability. CORS allows the Vite dev origin 5173.
- OpenAPI via springdoc at `/swagger-ui.html`.

### Tests

- `@WebMvcTest` per controller with mocked services, including pagination and filters.
- `DashboardApiIT` with Testcontainers PostgreSQL seeded with fixture rows: endpoints return the
  expected shapes.
- `LiveFeedTest`: leader change → feed reconnects to the new leader and keeps publishing.

### Acceptance checks

```bash
mvn -q verify
java -jar predisched-dashboard-api/target/predisched-dashboard-api.jar
curl localhost:8080/api/scheduler
curl localhost:8080/api/workers
```

### Docs

`docs/components/dashboard-api.md` with the endpoint table.
