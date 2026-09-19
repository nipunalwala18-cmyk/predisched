# Prompt 24: One-command deployment, demo script and final report (spec §16)

## Goal

`docker compose up --build` starts the whole system: 5 schedulers, 3 heterogeneous workers,
PostgreSQL, the prediction server, the dashboard API and the dashboard. A scripted demo walks through
all ten lab topics on the running product. The docs are complete enough for an examiner to reproduce
every result.

## Read first

- Spec §16, §17, §18, §19, §21, §22, `docs/LAB-COVERAGE.md` as it stands

## Build

1. **Images**: multi-stage Dockerfiles in `docker/` for the Java services (one image, role chosen by
   command), the prediction server, the dashboard (built static files served by nginx), plus the Spark
   and MPI images from prompts 12–14. Images run as non-root and have health checks.
2. **`docker/docker-compose.yml`**: services `scheduler-1`…`scheduler-5`, `worker-1`…`worker-3` with
   different `cpus:` / `mem_limit:`, `postgres` (volume, health check), `prediction`,
   `dashboard-api`, `dashboard`, `mock-http`. Ports from RULES. `depends_on` with health conditions.
   A `configs/docker.yaml` with service host names.
3. **Demo script** `scripts/demo.ps1` / `.sh`: a narrated walk through the ten lab topics on the
   running stack, each step printing the topic, running the real command and pausing for Enter:
   RPC submit → thread-pool parallelism → clock offsets and Lamport log → kill primary (election +
   failover) → stale read in eventual mode → strategy switch → Spark job → MPI collectives and
   matrix → predictive decision explain → open the dashboard. It must also work with `--no-pause`
   in CI as a smoke test.
4. **Docs**
   - Root `README.md`: what PrediSched is, architecture diagram, quick start (Docker and native),
     the benchmark results section from prompt 20, links to every component doc, and the finished
     progress table.
   - `docs/LAB-COVERAGE.md`: every row complete with code path, demo command and measured result.
   - `docs/REPORT.md`: the project report assembled from component docs and results: problem,
     design, per-experiment results, benchmark findings, limitations (spec §19), future work (§20).
     Every number cites the results file it came from.
   - `docs/TROUBLESHOOTING.md`: Windows port reservations, MS-MPI, winutils for Spark, Docker memory.
5. **CI**: a job that builds the images and runs `scripts/demo.sh --no-pause` against compose.

## Tests

- The compose smoke job in CI is the test: all services healthy within the timeout, the demo script
  completes, and `/api/overview` shows 3 workers and 1 leader.

## Acceptance checks

```bash
docker compose -f docker/docker-compose.yml up --build -d
```
```bash
docker compose -f docker/docker-compose.yml ps
```
```bash
scripts/demo.sh --no-pause
```

Paste the `ps` output with every service healthy and the full demo output. Then run
`docker stop predisched-scheduler-5` and paste the new leader from `/api/cluster/leader`.

## Docs and commit

- README progress table: every prompt ticked (mark 21 as skipped if it was).
- Commit: `Deployment: one-command Docker stack, lab demo script and final project report`
