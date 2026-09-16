# Prompt 00 — Foundation: repository, modules, contracts, config

**Spec sections:** §3.2, §6, §7, §14
**Depends on:** nothing
**Lab topics:** none directly (sets up the ground for all of them)
**Commit message:** `Foundation: multi-module skeleton, shared contracts, config and CI`

---

## Prompt

You are setting up the repository for PrediSched, a predictive distributed task scheduler. Read
`CLAUDE.md` and §3.2, §6, §7 and §14 of `docs/PROJECT-CONTEXT.md` first.

Create the skeleton the whole product grows into. No feature code yet.

### Build

1. Parent `pom.xml` (`com.predisched:predisched-parent`, packaging `pom`, Java 17) with
   `dependencyManagement` pinning gRPC 1.66.0, Protobuf 3.25.5, SLF4J 2.x, Logback 1.5.x,
   JUnit 5.10.x, Mockito 5.x, SnakeYAML 2.x, PostgreSQL JDBC 42.7.x, HikariCP 5.x. Plugins:
   compiler, surefire (JUnit 5), `os-maven-plugin` extension, `protobuf-maven-plugin` (xolstice 0.6.1)
   in `pluginManagement`, `exec-maven-plugin`.
2. Java modules (spec §7), each with a `pom.xml` and an empty `com.predisched.<name>` package:
   `predisched-common`, `predisched-client`, `predisched-scheduler`, `predisched-worker`,
   `predisched-election`, `predisched-replication`, `predisched-fault`, `predisched-benchmark`.
   `predisched-dashboard-api` arrives in Prompt 16.
3. `proto/` at the root. `predisched-common` compiles every `.proto` there
   (`protoSourceRoot = ${project.basedir}/../proto`) and exposes the generated classes. Start with
   `proto/common.proto`: package `predisched`, `java_multiple_files = true`,
   `java_package = "com.predisched.proto"`, and `message Ack { bool ok = 1; string message = 2; }`.
4. `predisched-common`:
   - `config/NodeConfig` — loads YAML from the first CLI argument or `PREDISCHED_CONFIG`. Fields:
     `nodeId`, `role` (`SCHEDULER` / `WORKER` / `CLIENT`), `host`, `port`, `peers` (list of
     id/host/port), `seed`, plus a free-form `settings` map for component options. Fails fast with a
     clear message on a missing file or missing `nodeId`.
   - `logback.xml` with pattern
     `%d{HH:mm:ss.SSS} [%X{node}] [L=%X{lamport}] %-5level %logger{20} - %msg%n`. The MDC is filled
     in Prompt 03.
   - `grpc/Channels` — a small cache of one `ManagedChannel` per `host:port`, closed on shutdown.
5. Python side: `requirements.txt` at the root (grpcio, grpcio-tools, protobuf, pandas, numpy,
   scikit-learn, xgboost, joblib, matplotlib, pyspark, mpi4py, psycopg[binary], pyyaml, pytest) and
   `scripts/gen-python-protos.(ps1|sh)` that generates stubs from `proto/` into `ml/generated/`,
   `mpi/generated/` and `spark/generated/`.
6. Top-level folders with a one-line `README.md` each: `ml/`, `spark/`, `mpi/`, `dashboard/`,
   `docker/`, `configs/`, `scripts/`, `docs/components/`, `results/`, `workloads/`.
7. `configs/local/`: `scheduler-1.yaml` … `scheduler-3.yaml` (ports 50051–50053, each listing the
   others as peers) and `worker-1.yaml` … `worker-3.yaml` (50261–50263, scheduler peers listed).
8. `docs/LAB-COVERAGE.md` already exists (planned locations). Leave its structure; later prompts fill
   in the evidence columns.
9. Root `README.md` already exists. Add a "Build status" line and keep the progress table; tick
   Prompt 00 when done.
10. `.github/workflows/ci.yml`: Temurin 17 + `mvn -B verify`; Python 3.11 + `pip install -r
    requirements.txt` (without mpi4py) + `pytest` once Python tests exist.

### Tests

`NodeConfigTest`: loads a valid file; rejects a missing file; rejects a file without `nodeId`.

### Acceptance checks

```bash
mvn -q verify
```

- Green, every module in the reactor summary.
- `predisched-common/target/generated-sources/protobuf` contains `Ack.java`.

### Out of scope

Any service, Docker, the database.
