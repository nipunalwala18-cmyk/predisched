# Dashboard API

Prompt 22, spec §15.10, §15.9, FR17, FR31, F7, F24. `predisched-dashboard-api` is a Spring Boot
3.3 service that everything the dashboard shows comes through:

- **stored history** from PostgreSQL (plain `JdbcTemplate`);
- **live state and admin controls** over gRPC to the primary scheduler;
- **a throttled STOMP stream** of cluster events.

```
java -jar predisched-dashboard-api/target/predisched-dashboard-api.jar [--dashboard.cluster-config=configs/dashboard.yaml]
```

## Pieces

- **`proto/admin.proto`: `AdminService` on every scheduler** (`AdminServiceImpl`). The mutating
  calls are answered only by the primary; the others reply `ok=false` with `leader=<id>`, and each
  admin action is an `ADMIN` event.
  - `SetStrategy`, `PauseQueue` / `ResumeQueue` (a new dispatcher pause), `DrainWorker` (the
    registry's scheduler-side drain) and `TriggerElection`.
  - `GetClusterState`: node, leader, peers, workers, queue depth, running attempts, strategy,
    model versions, pause state, arrival rate, decision p50/p95, live MAE, speculations,
    auto-scale mode, and task counts.
  - `StreamEvents`: a server-streaming feed of the node's event log, through a new `EventLog`
    listener hook.
- **Finding the primary.** The schedulers come from `dashboard.cluster-config`'s election peers,
  else from `dashboard.schedulers`. The primary is whichever answers `GetClusterState` with
  `leader=true`. It is cached until a call to it fails, then searched for again, so the API
  follows a failover by itself.
- **Reads**: `Repositories` holds all the SQL. Migration `V4` adds `predictions.pred_queue_len`,
  which the prediction server now writes, for the queue-forecast chart.
- **Subprocess jobs** (`Jobs`):
  - benchmark suites run in the background, with each finished run's line sent to `/topic/events`;
  - the what-if simulator and model promotion run synchronously.

  The simulator runs as a subprocess, not in-process as the prompt says: the benchmark module's
  artifact is a shaded jar, and pulling it into a Spring Boot jar would duplicate its gRPC and
  logging classes.
- **Docs and health**: OpenAPI at `/v3/api-docs` and `/swagger-ui`; actuator at
  `/actuator/health`.
- **CORS** allows the Vite dev server origin `http://localhost:5173`.

## Endpoints

| Method and path | Source | What it returns |
| --- | --- | --- |
| `GET /api/overview` | DB + primary | KPI strip (tasks/s, mean and p95 latency, SLA %, queue depth, active workers) and the cluster snapshot; `cluster: null` with `clusterError` when the cluster is down |
| `GET /api/workers` | DB + primary | Every worker with its last stored metrics and its live state |
| `GET /api/workers/{id}/history?minutes=` | DB | `worker_metrics` series |
| `GET /api/tasks?status=&type=&limit=` | DB | Tasks with latency |
| `GET /api/tasks/{id}` | DB + primary | Task row, lifecycle events, every placement decision with its breakdown, predictions, and the live `explain` |
| `POST /api/tasks` | primary | Submits (F24) with the caller's `Authorization` header forwarded; 202 accepted, 400 refused |
| `GET /api/queue/forecast?minutes=` | DB | Queued tasks per second, next to the M2 forecasts shifted by their 5 s horizon |
| `GET /api/predictions/accuracy?limit=` | DB | Recent `prediction_outcomes`, MAE per type, rolling MAE |
| `GET /api/models` | `registry.json` + meta + DB | Versions with status and test metrics, recent `DRIFT` events |
| `POST /api/models/{v}/promote?model=m1&force=` | registry CLI | The prompt 21 promotion rule; **409** when refused |
| `GET /api/cluster/leader` | every scheduler + DB | Leader, each node's view, the last election events |
| `GET /api/replication/status` | DB | Per node: last sequence number, lag behind the head |
| `GET /api/clock/offsets` | DB | Each node's last clock-sync offsets |
| `GET /api/benchmarks` | DB + results | Suites, runs, generated reports |
| `POST /api/benchmarks/run` | job | Starts `run-suite` (`{"scenario", "reps", "suiteId"}`); 202 with `runId`; progress on `/topic/events` |
| `GET /api/benchmarks/jobs/{id}` | job | Status and last output lines |
| `POST /api/simulate` | simulator | What-if run (`{"trace", "strategy", "speed", "workers", "seed"}`), labelled as a simulation |
| `POST /api/admin/strategy` | primary | `{"strategy": "..."}` |
| `POST /api/admin/drain/{workerId}` | primary | No new dispatches to that worker |
| `POST /api/admin/pause` | primary | `{"paused": true\|false}`; no body pauses |
| `POST /api/admin/election` | primary | Starts an election |
| `POST /api/chaos/{action}` | chaos service | `kill-worker`, `kill-primary`, `latency`, `cpu`, `partition`, `drain` (`{"node", "ms", "seconds", "threads"}`); `burst` (`{"tasks", "profile"}`) |

**Auth.** Admin, chaos, promote and benchmark-run calls need the header
`X-Admin-Key: <dashboard.admin-key>` (default `dev-admin`). Without it they get 401.

**Rate limit.** A token bucket allows `dashboard.admin-rate-per-second` (5) calls per second, in
bursts of twice that; past it, calls get 429 with `Retry-After`.

**Other errors.** Mutating calls the cluster refuses return 409. No reachable primary returns 503.
Read endpoints degrade instead of failing.

## WebSocket: `/ws/stream` (STOMP)

| Topic | Content |
| --- | --- |
| `/topic/metrics` | Every second: the primary's workers, queue depth, running attempts |
| `/topic/tasks` | Task lifecycle events: submit, enqueue, dispatch, execute, result, retries, speculation, prediction outcomes |
| `/topic/decisions` | `SCHEDULE_DECISION` events |
| `/topic/events` | Everything else: elections, chaos, drift, admin, scaling, benchmark progress |

`StreamBridge` follows the primary's `StreamEvents` and reconnects after a failover. `Throttler`
releases **one JSON-array batch per topic per 250 ms tick** (`dashboard.stream-rate-per-second`: 4,
per spec §15.11). A batch keeps the newest 500 items; anything older is counted as dropped.

## Acceptance run

The setup: `scripts/start-cluster.ps1 -Config configs/dashboard.yaml` (the chaos cluster with
PostgreSQL history), then the API with `--dashboard.cluster-config=configs/dashboard.yaml`. It
started in 8.9 s. The load was `POST /api/chaos/burst {"tasks": 60, "profile": "mixed"}` and three
`POST /api/tasks` (202 each).

```
$ curl -s localhost:8080/api/overview
{"generatedAt":"2026-09-27T10:49:52.172357900Z","kpis":{"windowSeconds":60,"tasksPerSecond":0.9833333333333333,
 "meanLatencyMs":875.7288135593220339,"p95LatencyMs":1603.2999999999997,"slaCompliancePct":100.0,"queueDepth":0,
 "activeWorkers":3},"cluster":{"answeredBy":"scheduler-5","leaderId":5,"strategy":"least_loaded","modelVersions":null,
 "paused":false,"queueDepth":0,"running":0,"arrivalRatePerSec":0.3,"decisionP50Ms":0.056,"decisionP95Ms":0.091,
 "liveMaeMs":null,"speculations":0,"autoscale":"none","tasksTotal":63,"tasksCompleted":59,"tasksFailed":4,
 "nodes":[{"id":1,"address":"localhost:51051","leader":false,"reachable":true}, ... ,
          {"id":5,"address":"localhost:51055","leader":true,"reachable":true}],
 "workers":[{"workerId":"worker-1","host":"localhost","port":51061,"poolSize":4,"healthy":true,"draining":false,
             "activeThreads":0,"queueLen":0}, ...],"atMs":1790506192732}}
```

The 4 failed tasks are the burst's `HTTP_TASK`s: no mock HTTP server was running for this demo.

```
$ curl -s -X POST -H "X-Admin-Key: dev-admin" -H "Content-Type: application/json" -d "{\"strategy\":\"round_robin\"}" localhost:8080/api/admin/strategy
{"ok":true,"previous":"least_loaded","current":"round_robin","message":"strategy changed"}
$ curl -s -X POST -H "X-Admin-Key: dev-admin" -H "Content-Type: application/json" -d "{\"strategy\":\"least_loaded\"}" localhost:8080/api/admin/strategy
{"ok":true,"previous":"round_robin","current":"least_loaded","message":"strategy changed"}
$ curl ... (no key) localhost:8080/api/admin/strategy   ->  401
```

**`GET /api/cluster/leader` before and after `POST /api/chaos/kill-primary`.** Each node below is
listed as (address, is it the leader, whom it believes leads). Before:

```
{"leader": 5, "leaderAddress": "localhost:51055"}
  nodes: (51051, false, 5) (51052, false, 5) (51053, false, 5) (51054, false, 5) (51055, true, 5)
```

The kill:

```
$ curl -s -X POST -H "X-Admin-Key: dev-admin" localhost:8080/api/chaos/kill-primary
{"message":"crashing","node":"scheduler-5","action":"kill-primary","untilMs":0,"ok":true}
```

6 s later. Scheduler 5 is now unreachable, and every survivor names 4:

```
{"leader": 4, "leaderAddress": "localhost:51054"}
  nodes: (51051, false, 4) (51052, false, 4) (51053, false, 4) (51054, true, 4) (51055 unreachable)
  elections: LEADER_ELECTED on scheduler-2, -3, -1, -4 at 10:50:04
```

The event stream followed the failover by itself. `StreamBridge` logged "Following the primary's
event stream" at 16:19:16 (scheduler 5) and again at 16:20:05 (scheduler 4).

**Every other endpoint answered 200:**

- `/api/workers`, `/api/tasks`, `/api/queue/forecast`, `/api/predictions/accuracy`, `/api/models`,
  `/api/benchmarks` (listing both suite reports), `/api/replication/status`, `/api/clock/offsets`
  and `/api/workers/worker-1/history`;
- `/actuator/health` (`UP`), `/v3/api-docs` and `/swagger-ui/index.html`;
- the CORS preflight from `http://localhost:5173`.

**Task detail.** `GET /api/tasks/api-c50e71c2` gave the lifecycle
`SUBMIT, ENQUEUE, SCHEDULE_DECISION, DISPATCH, RESULT, SLA_MET` and one stored decision. Its live
`explain` was not found: the task had been placed by the killed scheduler 5, whose in-memory log
died with it. The stored decision row is why the table exists.

**Simulation.** `POST /api/simulate {"strategy":"resource_aware","speed":10}` answered with the
simulator's metrics (mean 211.6 ms, p95 579 ms), labelled "discrete-event model, not a measurement
of the cluster".

## Tests

- `ControllersTest` (`@WebMvcTest`, mocked `Cluster`, `Repositories` and `Jobs`, 10 tests):
  - the overview shape, and the overview with the cluster down;
  - admin endpoints: 401 without or with a wrong key, 200 with it, chaos protected too;
  - the rate limit: most of 10 immediate calls get 429;
  - refused admin calls return 409;
  - chaos input checks;
  - tasks: list, 404 for an unknown task, 202 for a submission with `Authorization` forwarded,
    400 for an unknown type;
  - a refused promotion returns 409;
  - leader across schedulers, with one unreachable;
  - 503 when there is no primary.
- `IntegrationTest` (`@SpringBootTest` on a random port) runs against a throwaway migrated
  PostgreSQL database and an in-process scheduler with a real `SchedulerService`, `AdminService`
  and dispatcher over gRPC:
  - three `POST /api/tasks` complete;
  - `/api/overview` shows the live strategy, 3 completed tasks and 1 worker;
  - `POST /api/admin/strategy` makes the scheduler's dispatcher switch to least_loaded, and the
    overview reports it;
  - pause and resume reach the dispatcher;
  - the stored reads and actuator answer.

  Without `PREDISCHED_TEST_DB_URL` it is skipped; Testcontainers is not wired into this module.
- `StreamTest`:
  - the `Throttler` sends one batch per topic per tick, keeping the newest items;
  - a real STOMP client on `/ws/stream`, fed 400 events in 2 s (200 a second), receives all 400,
    in at most 5 messages in any second.
