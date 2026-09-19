# Prompt 05: Full task catalogue and the workload generator (spec §7, F8)

## Goal

Workers execute every task type in spec §7.2 that does not need a later component, and a workload
generator produces reproducible steady, bursty, periodic and mixed workloads saved as trace files that
can be replayed exactly.

## Read first

- Spec §7 (all), §4 FR4 and FR36, F8 in §6, §12.1 (workload patterns), RULES rule 8

## Build

1. **Executors** in `predisched-worker`, one class each, registered in `ExecutorRegistry`:
   - CPU-bound: `HASH_TASK` (`rounds`), `MONTE_CARLO_TASK` (`samples`, `seed`).
   - Memory-bound: `SORT_TASK` (`n`, `type=random|sorted|reversed`), `COMPRESS_TASK`
     (`size_mb`, `level`), `GRAPH_TASK` (`nodes`, `algo=bfs|pagerank`, `seed`).
   - I/O and network: `FILE_IO_TASK` (`size_mb`, `mode=write|read|both`, temp dir from config,
     file deleted afterwards), `HTTP_TASK` (`url`, `timeout`).
   - Every executor declares its `ResourceProfile`, parses its input strictly, and returns a
     deterministic, verifiable output (checksum, count or estimate).
   - Four types stay unregistered here and arrive later: `WORKFLOW_TASK` (prompt 09),
     `DB_QUERY_TASK` (11), `MAPREDUCE_TASK` (12) and `ML_INFER_TASK` (16). `IMAGE_TASK` is in the
     enum but not in the catalogue, so it stays reserved with no executor. A submit of an
     unregistered type is rejected by validation with a clear message.
2. **Mock HTTP service**: `scripts/mock-http.py` (stdlib `http.server`) with `/delay?ms=` and
   `/fail?rate=` endpoints, used by `HTTP_TASK` in demos and tests.
3. **Workload generator** in `predisched-client`:
   - `WorkloadProfile` definitions from spec §7.3 (`cpu_heavy`, `io_heavy`, `mixed`, `bursty`,
     `long_tail`, `deadline`) in `configs/workloads.yaml`: task mix weights, input-size ranges,
     priority distribution.
   - Arrival processes: `steady` (Poisson at rate r), `bursty` (idle, then a flood), `periodic`
     (sine-modulated rate). All draw from one seeded `Random`.
   - `generate` writes a trace file `workloads/<name>-<seed>.jsonl`: one line per task with offset ms
     from start, task id, type, input, priority, optional timeout.
   - `replay <trace> [--speed 1.0]` submits the trace against a scheduler with the original timing
     and writes per-task results (submit, start, end, worker, status) to a results CSV.
4. **CLI**: `predisched workload generate --profile mixed --pattern bursty --tasks 500 --seed 42`
   and `predisched workload replay workloads/mixed-bursty-42.jsonl`.

## Tests

- One test per executor: parses good input, rejects bad input, same input gives same output.
- `FILE_IO_TASK` leaves no temp files behind, including on cancellation.
- `WorkloadGeneratorTest`: same seed → byte-identical trace; different seed → different trace;
  mix proportions within ±5 % of config over 5,000 tasks.
- `ReplayTest`: replaying a 50-task trace against an in-process cluster completes all 50.

## Acceptance checks

```bash
java -jar predisched-client/target/predisched-client.jar workload generate --profile mixed --pattern bursty --tasks 300 --seed 42
```
```bash
java -jar predisched-client/target/predisched-client.jar workload replay workloads/mixed-bursty-42.jsonl
```

Paste the generator summary (count per type), a sha256 of the trace file from two runs with the same
seed (identical), and the replay summary (completed/failed per type, mean exec time per type).

## Docs and commit

- `docs/components/tasks-and-workloads.md`: catalogue table with measured mean exec time per type on
  this machine, profiles, trace format.
- Commit: `Workloads: full task catalogue and a seeded, replayable workload generator`
