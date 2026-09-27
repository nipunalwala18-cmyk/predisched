# Deployment: Docker stack and lab demo

Prompt 24, spec §16. One command starts the whole system: `docker compose -f
docker/docker-compose.yml up --build`. One script walks through the ten lab topics on the running
product: `scripts/demo.sh` (or `scripts/demo.ps1`).

> **Status on the development machine.** Docker is not installed on the laptop this project was
> built on, so **the compose stack has not been started here**. The acceptance checks that need
> Docker (`compose up`, `compose ps`, and `docker stop predisched-scheduler-5`) have not been run.
> They run in CI's `compose` job. The demo script *was* run end to end against the same system
> started natively, and its output is below.

## Files

| File | What it is |
| --- | --- |
| `docker/java.Dockerfile` | One image for every Java service. Maven builds in the first stage; the second is `eclipse-temurin:17-jre` running as uid 10001. The first argument picks the role (`docker/entrypoint.sh`): `scheduler`, `worker`, `dashboard-api`, `client` or `benchmark`. |
| `docker/prediction.Dockerfile` | The prediction server (`python:3.12-slim`, non-root). Model binaries are not committed, so the image trains m1–m3 from the committed `ml/data/dataset.parquet` at build time. A local run from an empty registry took 1 min 55 s and kept the same models as the committed metadata (ridge, mean per worker, logistic regression). |
| `docker/dashboard.Dockerfile`, `docker/nginx.conf` | The React build served by `nginx-unprivileged`. nginx also proxies `/api`, `/ws`, `/swagger-ui` and `/actuator` to the dashboard API, so the page and the API share one origin. |
| `docker/spark.Dockerfile`, `docker/mpi.Dockerfile` | From prompts 12–14. They are the compose `tools` profile, and `run-spark.sh` / `run-mpi.sh` fall back to them. |
| `docker/docker-compose.yml` | The stack (below). |
| `configs/docker.yaml` | `configs/dashboard.yaml` with service host names in place of `localhost`. |
| `.dockerignore` | Keeps local builds, venvs, `node_modules`, logs and model binaries out of the build context. |
| `scripts/demo.sh`, `scripts/demo.ps1` | The lab demo. The `.ps1` runs the `.sh` in Git for Windows' bash, so only one script has to be maintained. |

## The stack

| Service | Image | Host port | Limits | Health check |
| --- | --- | --- | --- | --- |
| `postgres` | `postgres:16`, volume `pgdata` | 5432 | | `pg_isready` |
| `prediction` | prediction | 50070 | | TCP connect to 50070 |
| `scheduler-1` … `scheduler-5` | java, `scheduler` | 51051–51055 | 1 CPU, 512 MB each | gRPC port open |
| `worker-1` | java, `worker`, pool 2, clock −300 ms | 51061 | 0.5 CPU, 384 MB | gRPC port open |
| `worker-2` | java, `worker`, pool 4, clock +500 ms | 51062 | 1 CPU, 512 MB | gRPC port open |
| `worker-3` | java, `worker`, pool 8, clock +200 ms | 51063 | 2 CPUs, 1 GB | gRPC port open |
| `dashboard-api` | java, `dashboard-api` | 8080 | 768 MB | `/actuator/health` |
| `dashboard` | dashboard (nginx) | 5173 → 8080 | | `GET /` |
| `mock-http` | `python:3.12-slim` running `scripts/mock-http.py` | 8100 | | `/healthz` |

Start-up order comes from health conditions in `depends_on`:
1. `postgres`, then `prediction`.
2. The five schedulers, which run the Flyway migrations.
3. The workers, once every scheduler is healthy, because a worker registers with all five.
4. `dashboard-api`, once scheduler-5 is healthy.
5. `dashboard`.

The containers are named `predisched-<service>`, so the spec's fault demos work as written:
`docker stop predisched-scheduler-5`, `docker stop predisched-worker-2`. All Java services and
the prediction server share the `logs` volume. There each node writes `logs/<node>.jsonl`, so
`merge-events.py` can merge the event logs of every node.

**Design choices**
- **One Java image.** Role by argument: the jars share nearly all their dependencies, and one
  image builds once.
- **Workers differ in CPU quota, memory and pool size** (spec §16). They also differ in clock
  offset, so the Berkeley step of the demo has something to correct.
- **The API reaches the dashboard through nginx, not a build-time URL.** An empty `VITE_API_URL`
  means "same origin". The dashboard derives the WebSocket URL from the page's location
  (`streamUrl` in `dashboard/src/api/client.ts`, tested in `components.test.tsx`).
- **Health checks.** A port check is enough for the gRPC nodes: a scheduler opens its port only
  after migrations and election wiring. The API uses Spring's health endpoint.
- **What the Java image does not contain.** The dashboard API's model jobs (retrain, benchmark)
  need Python, which is not in the Java image; in Docker those endpoints report the failure.
  - `MAPREDUCE_TASK`, `MATRIX_TASK mode=mpi` and `ML_INFER_TASK` also need Spark, MPI or Python on
    the worker, so they fail on Docker workers.
  - Every other task type runs.
  - The demo runs Spark and MPI in their own images instead.

## The demo

`scripts/demo.sh [--no-pause] [--docker|--native]`. It detects a running compose stack, otherwise
it assumes the native one. Each step prints the topic and a one-paragraph explanation, runs the
real command (echoed with `$`) and waits for Enter. With `--no-pause` it runs straight through
and exits 1 if any step failed, which makes it CI's smoke test.

| Step | Lab topic | What runs |
| --- | --- | --- |
| 1 | RPC (Exp 1) | `client submit` a `CPU_TASK`, then `client watch` |
| 2 | Multithreading (Exp 2) | 12 one-second `SLEEP_TASK`s by `submit-file`; wall time from the task records (`/api/tasks/{id}`) |
| 3 | Clocks (Exp 3) | the widest Berkeley round in the leader's log and its corrections; `merge-events.py --task` for step 1's task |
| 4 | Election + primary-backup (Exp 4, 8) | `cluster leader`, crash the leader (`stop-node.sh` / `docker kill`), wait for a new one, submit and complete a task, restart the old node |
| 5 | Consistency (Exp 5) | `predisched-benchmark consistency-compare --writes 100` |
| 6 | Load balancing (Exp 6) | `POST /api/admin/strategy {"strategy":"predictive"}` |
| 7 | Spark (Exp 7) | exports the live history, then `run-spark` `exec_stats.py` (the fixture CSV when the export cannot run) |
| 8 | MPI (Exp 9, 10) | `run-mpi.sh 4 collectives --generate 20`, `run-mpi.sh 4 matmul --size 400` |
| 9 | Predictive explain (F13) | a `CPU_TASK` under the predictive strategy, then `client explain` |
| 10 | Dashboard | checks `/api/overview` (3 workers) and `/api/cluster/leader` (1 leader), then opens `http://localhost:5173` |

In Docker the client and benchmark run inside the `dashboard-api` container, which the demo never
kills (`docker compose exec -T dashboard-api predisched client ...`). `merge-events.py` runs in
the prediction container, which has Python and the shared `logs` volume.

### Run it

Docker:

```bash
docker compose -f docker/docker-compose.yml up --build -d --wait
```
```bash
scripts/demo.sh
```

Native (Windows or Linux), from the repo root after `mvn -q verify`:

```bash
CLOCK_OFFSETS="-300 500 200" CONFIG=configs/dashboard.yaml scripts/start-cluster.sh
```
```bash
cd ml && ../.venv/Scripts/python -m predisched_ml.prediction_server --port 50070
```
```bash
java -jar predisched-dashboard-api/target/predisched-dashboard-api.jar --dashboard.cluster-config=configs/dashboard.yaml
```
```bash
scripts/demo.sh
```

`start-cluster.sh` has a new setting, `CLOCK_OFFSETS`, which gives worker *n* the *n*-th offset.

## Measured: the demo on the native stack

2026-09-27, Windows 11 laptop (4 cores / 8 threads). The stack: 5 schedulers and 3 workers on
`configs/dashboard.yaml`, the prediction server, the dashboard API and PostgreSQL 16. The command
was `scripts/demo.sh --no-pause`. Exit code 0, in **2 min 18 s**. The full output is in
`logs/p24-demo.txt`; the excerpts below are unedited apart from ANSI colours.

```
=== 1/10  Client-server communication with RPC (Exp 1) ===
$ client submit --type CPU_TASK --input n=2000000 --priority 5
accepted=true task_id=task-cb1ce937 ... message='queued'
$ client watch task-cb1ce937
task_id=task-cb1ce937 status=COMPLETED worker=worker-2 exec_ms=31 ... result='primes_below_2000000=148933'

=== 2/10  Multithreading: worker thread pools (Exp 2) ===
submitted 12 tasks; waiting for them through the dashboard API
12 tasks, 12.1 s of execution, done 3.3 s after the first submission (3.7 running at once on average); per worker: worker-1 1, worker-2 2, worker-3 9

=== 3/10  Clock synchronisation: Berkeley and Lamport (Exp 3) ===
$ grep 'Berkeley round' <scheduler-4 log>
the widest spread this leader measured, and the corrections it sent:
Berkeley round: offsets before {scheduler-4=0, worker-1=103, worker-2=103, worker-3=101} (spread 103 ms), average 76 ms, excluded []
Berkeley round: corrections {scheduler-4=76, worker-1=-27, worker-2=-27, worker-3=-25}, offsets after {...=76} (spread 0 ms)

=== 4/10  Leader election and primary-backup failover (Exp 4, Exp 8) ===
scheduler 1..5: leader=4
$ kill_scheduler 4
stopped scheduler-4 (pid 1579)
new leader: scheduler-5 after about 2 s
task_id=task-122588bf status=COMPLETED worker=worker-1 exec_ms=20 ... result='primes_below_1000000=78498'
restarting scheduler-4: it rejoins as a backup and catches up with SyncFrom
scheduler 1..5: leader=5

=== 5/10  Replication and consistency models (Exp 5) ===
mode      down  failed  write_p50_ms  write_p95_ms  stale_reads  convergence_ms
strong       0       0          6.90          8.20            0               1
strong       1       0         62.17         64.24            0               0
eventual     0       0          0.16          0.33          100              77
eventual     1       0          0.11          0.23          100              80

=== 6/10  Load balancing: switch strategy at runtime (Exp 6) ===
{"ok":true,"previous":"least_loaded","current":"predictive","message":"strategy changed"}

=== 7/10  MapReduce with Spark (Exp 7) ===
exported the live execution history: 72330 executions
run-spark: native (C:\CODING\predisched\.venv\Scripts\spark-submit.cmd)
  COMPRESS_TASK     4988   410.3   ...   SLEEP_TASK  9775   383.8   ...   (11 types, 5 workers)

=== 8/10  MPI collectives and matrix multiplication (Exp 9, Exp 10) ===
RESULTS 20 written to ..\logs\demo-exp9.csv
check: np.allclose(C, A @ B) = True (max |diff| 0.00e+00) rows per rank [100, 100, 100, 100]
size=400 procs=4 seed=42 checksum=16009981.328371 scatter_bcast_ms=6.475 compute_ms=4.395 gather_ms=1.766 total_ms=12.107

=== 9/10  Predictive scheduling: explain a decision (F13) ===
task task-2741426d: strategy predictive chose worker-3 in 79.00 ms
models: m1=v1,m2=v1,m3=v1
  worker       pred_queue   pred_wait   pred_exec  overload     penalty        cost
  worker-1          0.22      27.8 ms     25.8 ms     0.001      0.0 ms     53.6 ms
  worker-2          0.00       0.0 ms     23.8 ms     0.001      0.0 ms     23.8 ms
* worker-3          0.00       0.0 ms     23.6 ms     0.001      0.0 ms     23.6 ms

=== 10/10  The dashboard ===
/api/overview: activeWorkers=3; /api/cluster/leader: 1 node(s) leading
Demo finished: all ten steps ran.
```

Reading it:

- **Step 2.** Twelve seconds of sleeping finished 3.3 s after the first submission, with 3.7 tasks
  running at once on average. Least loaded sent most of them to worker-3 because its pool had the
  most free threads.
- **Step 3: the Berkeley spread is only 103 ms here.** The leader had just changed in an earlier
  run, so worker offsets were already mostly corrected. Before that, on the same workers started
  with `CLOCK_OFFSETS="-300 500 200"`, scheduler-5 measured and corrected an 804 ms spread:
  `offsets before {scheduler-5=0, worker-1=-263, worker-2=541, worker-3=240} (spread 804 ms)`, then
  `offsets after ... (spread 0 ms)`.
- **Step 9: the 79 ms decision is the first predictive call after the switch.** The client is
  cold at that point. Steady-state decisions take 6–10 ms (`predictive-strategy.md`).

**A third run, after the speculation fix below, on freshly restarted nodes and rebuilt jars:**
- Exit 0 in 3 min 40 s.
- Step 2: 12.3 s of work done 3.5 s after the first submission.
- Step 3: scheduler-3 was leading. It measured `offsets before {scheduler-3=0, worker-1=-259,
  worker-2=546, worker-3=236} (spread 805 ms)` and brought the spread to 0 ms in one round.
- Step 4: scheduler-2 took over about 2 s after the kill.
- Step 10: 3 workers and 1 leader.

**The full `mvn verify` in this prompt exposed a real race in speculative execution**
(prompt 19). The build was running beside the 8 cluster JVMs.
- A speculative copy could be dispatched after the original had already closed its race, and
  then run unsupervised.
- `SpeculationTest` failed about 1 run in 5 under that load, and passed 10 of 10 after the fix.
- Details are in [speculation-and-chaos.md](speculation-and-chaos.md).

A first run took 4 min 25 s and exposed three problems in the script, all fixed:
- Step 2 started one client JVM per `watch`, so its "wall time" was mostly JVM start-up. It now
  reads the task records.
- Step 4 waited 60 s for the restarted node to win an election. A restarted scheduler rejoins as
  a backup, so the step now waits for it to answer instead.
- `merge-events.py` also read the prediction server's `logs/predictions.jsonl` and printed a
  `0 ?` row. It now skips records without a node and a Lamport time.

## Tests

- **CI `compose` job** (`.github/workflows/ci.yml`), after `build` and `dashboard`:
  - `docker compose up --build -d --wait --wait-timeout 600`, so every service must be healthy;
  - `docker compose ps`;
  - `scripts/demo.sh --no-pause --docker`;
  - a check that `/api/overview` shows 3 workers and `/api/cluster/leader` 1 leader;
  - logs on failure, then `down -v`.
- **`components.test.tsx` `streamUrl`**: the WebSocket URL from an absolute API URL, and from the
  page's origin over http and https.
- **Native demo run** above: all ten steps passed.
- **Same-origin dashboard build.** `VITE_API_URL= npm run build` produces a bundle with no API
  host in it. A small Node server played nginx's part: it served `dist/` on port 5174 and
  proxied `/api`, `/ws` and `/actuator` to the running API. In the browser the header showed
  `LIVE` (the STOMP WebSocket was connected), `Leader: S5` and `Strategy: PREDICTIVE`. Every API
  call went to `http://localhost:5174/api/...`, and the console had no errors. nginx itself was
  not run.

## Not done here

- **The acceptance checks that need Docker**: `compose up`, `compose ps` with every service
  healthy, the demo against compose, and `docker stop predisched-scheduler-5` followed by
  `/api/cluster/leader`. They need a machine with Docker, or the CI job. `docs/TROUBLESHOOTING.md`
  covers the memory Docker Desktop needs.
- **Two paths are untested because no image has been built:** the `docker kill` failover, and the
  Spark and MPI images started from the demo.
