# Prompt 00: Bootstrap the repository

## Goal

An empty repo becomes a building multi-module Maven project with the shared `proto/` folder, the
agent rules at the root, the docs skeleton and CI. Nothing runs yet, but `mvn -q verify` is green.

## Read first

- `spec/RULES.md` (all of it)
- Spec §1 Project Overview, §3.2 Components, §8 Technology Stack, §9 Repository Structure

## Build

1. **Root files**
   - `CLAUDE.md`: a copy of `spec/RULES.md`, with a first line saying the canonical copy lives in
     `spec/RULES.md`. Agents read it automatically.
   - `.gitignore` for Java (`target/`), Python (`__pycache__/`, `.venv/`, `*.pyc`), Node
     (`node_modules/`, `dist/`), IDE folders, `logs/`, `ml/data/raw/`, `ml/models/*.joblib`.
   - `.editorconfig`: UTF-8, LF, 4 spaces for Java and Python, 2 for TS/JSON/YAML.
   - `README.md`: one-paragraph summary (spec §22), a quick-start placeholder, and a **progress
     table** listing prompts 00–24 with an unticked box each. Tick 00.
2. **Parent `pom.xml`** (`com.predisched:predisched-parent`, packaging `pom`)
   - Java 17, UTF-8, `dependencyManagement` pinning gRPC 1.66.0, Protobuf 3.25.5, JUnit 5.10.x,
     Mockito 5.x, SLF4J 2.x, Logback 1.5.x, SnakeYAML 2.x.
   - `pluginManagement` for `maven-compiler-plugin`, `maven-surefire-plugin` (JUnit 5), and
     `protobuf-maven-plugin` (`org.xolstice.maven.plugins`) with `os-maven-plugin` as an extension.
3. **Java modules** (spec §9), each with a `pom.xml` and an empty `src/main/java/com/predisched/<module>`:
   `predisched-common`, `predisched-client`, `predisched-scheduler`, `predisched-worker`,
   `predisched-election`, `predisched-replication`, `predisched-fault`, `predisched-benchmark`.
   Do not add `predisched-dashboard-api` yet (prompt 22).
   - `predisched-common` runs `protobuf-maven-plugin` over `${project.basedir}/../proto` and exports
     the generated classes. Every other module depends on `common` only.
4. **`proto/`** with a single `common.proto` holding `message Ack { bool ok = 1; string message = 2; }`
   (package `predisched`, `option java_multiple_files = true`, `option java_package =
   "com.predisched.proto"`). Later prompts add the other files.
5. **Placeholder folders** with a one-line `README.md` each: `spark/`, `mpi/`, `ml/`, `dashboard/`,
   `docker/`, `configs/`, `scripts/`, `results/`, `workloads/`.
6. **Docs skeleton**
   - `docs/LAB-COVERAGE.md`: a table with one row per lab experiment (spec §1.4), columns
     *Exp · Topic · Code · Demo command · Measured result*, all cells "not started" except topic.
   - `docs/components/README.md`: explains that each component gets one file here.
7. **CI**: `.github/workflows/ci.yml` running `mvn -q verify` on Temurin 17 for pushes and PRs.
   Later prompts add Python and Node jobs.

## Tests

- One trivial test in `predisched-common` that builds an `Ack` proto, proving code generation works.

## Acceptance checks

```bash
mvn -q verify
```
```bash
mvn -q -pl predisched-common dependency:tree
```

Paste both outputs. The generated `Ack` class must be on the `common` classpath.

## Docs and commit

- README progress table: tick 00.
- Commit: `Bootstrap: multi-module Maven build, shared protos, docs skeleton and CI`
