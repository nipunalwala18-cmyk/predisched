# Troubleshooting

Problems met while building and running PrediSched, mostly on Windows, and what fixed them. Each
entry links to the component doc with the full story.

## Ports

**Symptom.** A node fails at start with `Failed to bind to address` or
`An attempt was made to access a socket in a way forbidden by its access permissions`, although
nothing is listening on the port.

**Cause.** On Windows, Hyper-V, WSL2 and Docker Desktop reserve blocks of TCP ports, and the
blocks can change after a reboot. See which ports are reserved now:

```bash
netsh interface ipv4 show excludedportrange protocol=tcp
```

The spec's ports (50051–50055 and so on) sat inside an excluded 50000–50159 block on the
development laptop. So on 2026-09-20 (prompt 01) the whole group moved +1000:

- schedulers 51051–51055;
- workers 51061–51063.

The table in `spec/RULES.md` records this. The prediction server kept 50070, which was free
when it was added. If a reservation covers it, start the server with `--port <free port>` and
set `prediction.port` to match in the config you run.

**Fix.** Pick a range that is free, change every port of the group in `configs/`, and record the
move in the RULES table. Alternatively, reserve the ports before Hyper-V takes them. This needs
an elevated prompt:

```bash
netsh int ipv4 add excludedportrange protocol=tcp startport=51051 numberofports=20
```

**Symptom.** `Address already in use` for a port. A node from an earlier run is still alive.

`scripts/stop-node.sh all` stops everything `start-cluster` started. For anything else, find the
owner with PowerShell and stop it:

```powershell
Get-NetTCPConnection -State Listen -LocalPort 51051 | Select-Object OwningProcess
```

## MPI on Windows (MS-MPI and Intel MPI)

**Symptom.** `mpi4py` fails to import with `DLL load failed`, or `mpiexec` is not found.

- **The default on Windows is `impi_rt`**, Intel's MPI runtime, installed from PyPI with
  `pip install -r mpi/requirements.txt`. It puts `mpiexec.exe` and `impi.dll` in
  `.venv\Library\bin`. `scripts/run-mpi.ps1` and `run-mpi.sh` find it there.
  - You need no admin rights and no `hydra_service` for local runs.
  - Mind the versions: mpi4py 3.1 wheels load only MS-MPI, which is why mpi4py is pinned to 4.1.
- **MS-MPI**, which the spec names, needs two installers from Microsoft: `msmpisetup.exe` (the
  runtime) and `msmpisdk.msi` (the SDK). Open a new terminal afterwards so that `mpiexec` and
  `MSMPI_BIN` are on the environment. `run-mpi` prefers an `mpiexec` on `PATH`.
- **Four ranks on a 4-core laptop are slower than expected.** They share the cores with the OS.
  A lone compute block takes 8.4 ms, and 14–18 ms when four run at once
  ([components/mpi.md](components/mpi.md)).
- **Linux:** `sudo apt-get install openmpi-bin libopenmpi-dev`. OpenMPI refuses to start more
  ranks than cores unless `OMPI_MCA_rmaps_base_oversubscribe=1` is set; CI and `mpi.Dockerfile`
  set it.

## Spark on Windows: winutils and friends

Full notes in [components/spark.md](components/spark.md#windows).

- **`Did not find winutils.exe` / `HADOOP_HOME is unset`.** This warning is harmless for
  PrediSched's jobs. They never use Spark's Hadoop file writers, and results go through the
  driver. For a job that does write with Spark, you need a Hadoop 3.3 `winutils.exe` and
  `hadoop.dll` in `C:\hadoop\bin`, with `HADOOP_HOME=C:\hadoop`. Get those binaries from a source
  you trust, or use the Docker route instead (`scripts/run-spark.sh` falls back to
  `docker/spark.Dockerfile` when there is no native `spark-submit`).
- **`Python worker exited unexpectedly (crashed)` on every task.** Under Python 3.12,
  PySpark 3.5.3's worker can lose its last records at interpreter shutdown. `spark/predisched_spark/pyworker.py`
  flushes explicitly, and `common.configure` selects it on Windows.
- **`No module named 'distutils'`** from `toPandas`: install `setuptools` (pinned in
  `spark/requirements.txt`).
- **From Git Bash** use `scripts/run-spark.ps1`. The `.sh` looks for a Unix `spark-submit` and
  does not know about `.venv\Scripts\spark-submit.cmd`; `scripts/demo.sh` switches to the `.ps1`
  by itself on Windows.

## Docker memory

The compose stack sets these memory limits:

| Services | Limit |
| --- | --- |
| 5 schedulers | 512 MB each |
| 3 workers | 384 MB, 512 MB and 1 GB |
| Dashboard API | 768 MB |
| PostgreSQL, prediction server, nginx | no limit set |

- **Docker Desktop needs about 6 GB** for everything to start. Actual use when idle is about
  3 GB.
  - On WSL2, Docker gets whatever WSL is allowed, which by default is half the RAM. Raise it in
    `%UserProfile%\.wslconfig`, then restart WSL:
    ```
    [wsl2]
    memory=8GB
    ```
  - With Hyper-V or on macOS, use Settings → Resources.
- **Heap follows the limit.** The Java image sets `-XX:MaxRAMPercentage=75`, so a JVM's heap
  follows its container limit. A scheduler killed with exit code 137 and `OOMKilled: true` in
  `docker inspect` needs a higher `mem_limit` for that service.
- **The prediction image trains its models during the build.** That takes about 2 minutes and
  about 1 GB of memory, so a slow first `up --build` is expected.
- **Build context.** `.dockerignore` keeps `node_modules`, `target/`, `.venv` and logs out of
  it. If the "Sending build context" step is slow, check that the file is at the repo root.

## Line endings

**Symptom.** A container exits at once with `/usr/bin/env: 'bash\r': No such file or directory`.

**Cause.** A shell script was checked out with CRLF (`core.autocrlf=true` on Windows) and copied
into an image.

**Fix.** `.gitattributes` forces `*.sh` and `docker/nginx.conf` to LF. After changing it, run
`git add --renormalize .` once.

## PostgreSQL

- **`Connection to localhost:5432 refused` in `mvn verify` (`StorageTest`).** The DB tests use
  `PREDISCHED_TEST_DB_URL` when it is set, and PostgreSQL is not running. Start it (`pg_ctl
  start`, or the `docker run ... postgres:16` line in RULES), or unset the variable so the tests
  use Testcontainers or skip.
- **`the database system is not yet accepting connections`** right after a crash: PostgreSQL is
  replaying its WAL. `pg_ctl start -w` waits for it.

## The cluster

- **`cluster leader` shows followers as `unreachable (DEADLINE_EXCEEDED)` right after start.**
  They are still starting (catch-up, migrations). Wait a few seconds and ask again.
- **Every predictive decision falls back to least_loaded.** Either the prediction server is not
  running, or the circuit breaker is open because calls miss their deadline.
  - With many JVMs and a browser on one laptop, 10 ms is too tight. `configs/dashboard.yaml` and
    `configs/docker.yaml` use 50 ms.
  - `client explain <task>` shows the fallback reason.
- **A restarted scheduler does not become leader again.** This is by design: it rejoins as a
  backup, catches up and waits for the next election
  ([components/fault-tolerance.md](components/fault-tolerance.md)).
