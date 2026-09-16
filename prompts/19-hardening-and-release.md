# Prompt 19 — Hardening, lab coverage and release

**Spec sections:** §5 (all), §15, §16, §17, §19, §20
**Depends on:** 18
**Lab topics:** all ten, verified end to end
**Commit message:** `Release: end-to-end verification, lab coverage evidence, report and demo`

---

## Prompt

Read `CLAUDE.md` and the whole of `docs/PROJECT-CONTEXT.md`. Prove the product meets its requirements,
close the gaps you find, and package it for submission and demo. Fix defects you find; do not add
features.

### 1. Requirements audit

Create `docs/REQUIREMENTS-TRACE.md`: one row per FR1–FR21 and per NFR in spec §5 with the code
location, the test that proves it, and the evidence (command + result). Anything without a test gets
one now. Anything that fails is fixed, or listed honestly under "Known gaps" with the reason.

### 2. Stress and correctness

- 30-minute soak on the Docker stack with the `mixed` trace looped: no memory growth trend in any JVM
  (record heap after GC every minute), no lost tasks, telemetry queue drops reported.
- Chaos run: randomly kill and restart one scheduler or worker every 60–120 s for 20 minutes while a
  workload runs. Invariants checked at the end from the DB and worker logs: every accepted task is
  COMPLETED, CANCELLED or FAILED-with-attempts-exhausted; zero duplicate executions; exactly one leader at
  the end; replicas identical.
- Run the concurrency tests under repetition (`-Dsurefire.rerunFailingTestsCount=0`, 20 iterations)
  to flush out flakiness; fix any flaky test's cause, never by adding sleeps.

### 3. Lab coverage evidence

Finish `docs/LAB-COVERAGE.md`. For each of the ten topics: where it lives in the product, why it is
needed there, the one command that demonstrates it on the running stack, a trimmed real output, and
the measured result. If the course needs a separate write-up per topic, generate `docs/lab/<topic>.md`
(aim, theory, how PrediSched implements it, procedure, output, result) from the component docs and
real outputs — no invented numbers, and no separate code.

### 4. Report and presentation

- `docs/REPORT.md`: abstract, research question (§1.2), architecture, each component, ML method and
  results, benchmark method and results with significance, limitations (§17, updated with what was
  observed), future work (§18).
- `docs/DEMO.md`: a 10-minute demo script — start stack, submit workload, show dashboard, switch
  strategies, show predictions, kill primary, kill worker, show zero loss, show benchmark summary, show
  MPI and Spark outputs. Each step has the exact command and what the audience should see.
  `scripts/demo` follows it.
- Root `README.md` final form: overview, architecture, quick start, lab coverage table, headline
  benchmark results (measured), repository map, how to reproduce every result, license.
- Resume bullets (spec §19) rewritten with the measured numbers.

### 5. Release

- CI green on `main`. Tag `v1.0.0`. GitHub release notes summarising components and headline results,
  with the benchmark summary attached.

### Acceptance checks

- `docs/REQUIREMENTS-TRACE.md` has no row without evidence.
- Soak and chaos invariants pass, with outputs saved under `results/hardening/`.
- A fresh clone on a clean machine reaches a running dashboard by following only the README.
