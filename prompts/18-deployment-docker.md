# Prompt 18 — Deployment: Docker Compose and operator scripts

**Spec sections:** §5 (Usability), §14
**Depends on:** 17
**Lab topics:** none directly (runs every topic as real separate nodes)
**Commit message:** `Deployment: one-command Docker stack with heterogeneous workers and fault drills`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Make the whole product start with one command, with
every scheduler and worker as its own container, and make the fault demos one command each.

### Images (`docker/`)

- `java-node.Dockerfile` — multi-stage: Maven build with a dependency-cache layer, then
  `eclipse-temurin:17-jre`; one image runs scheduler, worker or client depending on `PREDISCHED_ROLE`.
  The worker image includes OpenMPI, Python and the `mpi/` package so the MPI backend works in
  containers.
- `prediction.Dockerfile` — `python:3.11-slim`, installs only serving dependencies, copies
  `ml/models`.
- `dashboard-api.Dockerfile`, `dashboard.Dockerfile` (build with Node, serve with `nginx:alpine`,
  proxying `/api` and `/ws` to the API).
- All images run as a non-root user and define a `HEALTHCHECK`.

### Compose (`docker/docker-compose.yml`)

- Services: `postgres` (volume, healthcheck), `scheduler-1..3`, `worker-1..3`, `prediction`,
  `dashboard-api`, `dashboard`. Ports as spec §14.
- Worker heterogeneity through `cpus:` and `mem_limit:` (e.g. 0.5 / 1.0 / 2.0 CPUs) plus matching
  `poolSize` — mirrors the `heterogeneous-3` profile.
- Configuration through environment variables and mounted `configs/docker/*.yaml`; no host-specific
  paths.
- `depends_on` with `condition: service_healthy`, so `up` is ordered and repeatable.
- `docker/docker-compose.scale.yml` override for 5 schedulers / 5 workers.
- `docker/docker-compose.spark.yml` with a Spark container that runs the Prompt 11 jobs against the
  compose database (the no-Windows-setup path for analytics).

### Operator scripts (`scripts/`, both `.ps1` and `.sh`)

- `up` / `down` / `logs <service>` / `status` (leader, workers, prediction health).
- `drill-kill-primary` — `docker stop` the current leader (found through the API), then prints the new
  leader and time to elect.
- `drill-kill-worker <n>` — stops a worker mid-workload and shows its tasks being reassigned.
- `run-benchmark <suite>` — runs Prompt 15's suite against the compose stack.
- `demo` — the full scripted walkthrough from Prompt 19, step by step with pauses.

### Tests / CI

- A GitHub Actions job builds all images and runs `docker compose up -d`, waits for health, submits 50
  tasks through the client container, asserts all complete, runs `drill-kill-primary`, asserts no task
  lost, and tears down.

### Acceptance checks

```bash
scripts/up.sh
scripts/status.sh
scripts/drill-kill-primary.sh
scripts/drill-kill-worker.sh 2
scripts/down.sh
```

Open http://localhost:5173 (or the nginx port) during the drills and confirm the dashboard follows.

### Docs

`docs/components/deployment.md`; update the root README "Quick start" to the one-command path.
