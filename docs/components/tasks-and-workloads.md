# Task catalogue and workloads (feature F8)

Two things live here: the set of task types a worker can actually execute, and the seeded generator
that produces workloads as trace files which can be replayed exactly. Together they give every
later prompt a fixed vocabulary of tasks to schedule and a fixed workload to schedule them on.

Built by `spec/prompts/05-task-catalogue-workloads.md`. Spec sections: §7 (all), §4 FR4 and FR36,
F8 in §6, §12.1, rule 8.

## The catalogue

Ten types have an executor, one class each in `predisched-worker`, all registered in
`ExecutorRegistry`:

| Type | Profile | Input (required, then optional with default) | Verifiable output | Mean exec ms* |
| --- | --- | --- | --- | --- |
| `CPU_TASK` | `CPU_BOUND` | `n` (2–1e8) | `primes_below_<n>=<count>` | 22.0 |
| `SLEEP_TASK` | `IO_BOUND` | `ms` (0–60000), `failRate`, `seed` | `slept_ms=<ms>` | 375.0 |
| `MATRIX_TASK` | `PARALLEL` | `size` (1–1000), `threads=1` | `size=.. threads=.. checksum=<hex>` | 42.1 |
| `HASH_TASK` | `CPU_BOUND` | `rounds` (1–2e7) | `rounds=.. digest=<sha256>` | 69.9 |
| `MONTE_CARLO_TASK` | `CPU_BOUND` | `samples` (1–5e8), `seed=42` | `samples=.. inside=.. pi_estimate=<6dp>` | 318.7 |
| `SORT_TASK` | `MEMORY_BOUND` | `n` (2–2e7), `type=random\|sorted\|reversed` | `n=.. type=.. sorted=true sum=.. first=.. last=..` | 66.6 |
| `COMPRESS_TASK` | `MEMORY_BOUND` | `size_mb` (1–512), `level=6` | `raw_bytes=.. compressed_bytes=.. ratio=.. sha256=..` | 417.0 |
| `GRAPH_TASK` | `MEMORY_BOUND` | `nodes` (2–5e6), `algo=bfs\|pagerank`, `seed=42` | `visited=..` or `rank_sum=1.000000 top_node=.. top_rank=..` | 102.7 |
| `FILE_IO_TASK` | `IO_BOUND` | `size_mb` (1–2048), `mode=write\|read\|both` | `written_bytes=.. read_bytes=.. sha256=..` | 100.3 |
| `HTTP_TASK` | `NETWORK_BOUND` | `url` (http/https), `timeout` (1–120000 ms) | `status=.. bytes=.. sha256=..` | 188.6 |

\* Measured on this machine (Intel Core i3-8130U, 2 cores / 4 logical, 23.9 GB RAM, project bytecode
target 17, run on OpenJDK 21.0.12) during the 300-task bursty replay below, with a worker pool of 4.
The means are therefore loaded means over the size range each profile drew, not isolated timings:
`SLEEP_TASK`, `COMPRESS_TASK`, `MONTE_CARLO_TASK` and `HTTP_TASK` are dominated by their size knob,
while `CPU_TASK` and `MATRIX_TASK` are small enough that thread contention matters more than the work
itself. The CSV with every task is `results/mixed-bursty-42-replay.csv`.

## Design choices

- **One class per type, selected by registry, not by branching** (rule 4). `ExecutorRegistry` holds a
  `ConcurrentHashMap<TaskType, TaskExecutor>`; adding a type is a new class plus one `register`
  call. The scheduler never learns any of this: it validates input and hands over a string.
- **The input rules live in `predisched-common`, not in the worker.** `TaskInputSpec` is one table
  of required keys, ranges, enums, fractions and URL shapes, used twice: the scheduler rejects a
  malformed submit before it ever reaches a worker (FR2), and each executor calls
  `TaskInputSpec.firstError` before doing any work. Both sides therefore agree on "well-formed"
  without the scheduler importing a worker class (rule 3).
- **Every executor returns something checkable.** A checksum, a count, an estimate, `sorted=true`,
  `rank_sum=1.0`, an HTTP status. A task that returns "ok" is useless to the dataset in prompt 15;
  these return a value a test can assert and a reader can verify by hand.
- **Every executor is deterministic for the same input.** Randomness is always seeded (`SORT_TASK`
  and `COMPRESS_TASK` use a fixed `Random(42)` fixture, `MONTE_CARLO_TASK` and `GRAPH_TASK` take the
  seed from the input), so replaying a trace twice produces the same outputs and the same digest.
- **Long loops poll for interruption** (`if ((i & 0xFFFF) == 0 && Thread.currentThread().isInterrupted())`)
  and throw `CancellationException`, so cancelling a 20-million-round hash stops it promptly instead
  of running to completion.
- **`FILE_IO_TASK` deletes its file in a `finally` block**, including when it is cancelled
  mid-write, so a cancelled task leaves nothing in the worker's temp dir. The temp dir is
  `<java.io.tmpdir>/predisched-fileio-<workerId>` unless `worker.fileIoDir` overrides it, and
  `WorkerMain` creates it at startup so a bad path fails fast.
- **Four types stay unregistered** and say why. `WORKFLOW_TASK` (prompt 09), `DB_QUERY_TASK` (11),
  `MAPREDUCE_TASK` (12) and `ML_INFER_TASK` (16) have no executor, and `IMAGE_TASK` is in the enum
  but not in the catalogue. Validation rejects all five with the prompt they arrive in, instead of
  accepting a task that would then fail on a worker.

## Mock HTTP service

`scripts/mock-http.py` is stdlib-only, so nothing has to be installed to demo or test `HTTP_TASK`:

```bash
python scripts/mock-http.py --port 8100
```

| Endpoint | Behaviour |
| --- | --- |
| `GET /delay?ms=N` | Sleeps N ms (0–120000), then 200 with the fixed body `predisched-ok` |
| `GET /fail?rate=R` | 500 with probability R (0.0 always succeeds, 1.0 always fails); `seed=` makes the draw reproducible |
| `GET /healthz` | 200 `ok`, used for the readiness poll |

A non-2xx response is a task failure, which is what makes `/fail?rate=1.0` a deterministic
retry-and-dead-letter demo against the real scheduler.

## Workload profiles

`configs/workloads.yaml` holds the six profiles of spec §7.3. Each profile is a weighted task mix, a
size range per type, a priority distribution and an optional deadline fraction:

| Profile | Mix | Point of it |
| --- | --- | --- |
| `cpu_heavy` | 80 % `CPU_TASK` / `HASH_TASK` / `MONTE_CARLO_TASK`, 10 % `MATRIX_TASK` | Pure compute scheduling, where the current metrics are most informative |
| `io_heavy` | 70 % `FILE_IO_TASK` / `HTTP_TASK` / `SLEEP_TASK` plus `COMPRESS_TASK` | Tests whether the scheduler over-trusts low CPU readings |
| `mixed` | 10 % each of all ten executors | The realistic, hardest-to-predict case |
| `bursty` | 40 % `MATRIX_TASK`, 30 % `SORT_TASK`, 15 % each `CPU_TASK` / `SLEEP_TASK` | Spike handling |
| `long_tail` | 40 % `SLEEP_TASK`, 30 % `CPU_TASK`, few very large `SORT_TASK` / `GRAPH_TASK` | Straggler handling (F11) |
| `deadline` | mixed, 50 % high priority, 80 % carrying a 2–15 s deadline | SLA compliance and priority ageing |

What "size" means depends on the type (`n`, `rounds`, `samples`, `size_mb`, `nodes`, mock delay ms);
the header comment in the file spells out the mapping. Enum knobs (`SORT_TASK` order, `GRAPH_TASK`
algorithm, `FILE_IO_TASK` mode, `COMPRESS_TASK` level) are drawn uniformly, and `HTTP_TASK` is
pointed at `httpBaseUrl + "/delay?ms=<size>"`.

The three arrival processes (`ArrivalPattern`) all draw from the one seeded `Random`:

| Pattern | Shape |
| --- | --- |
| `steady` | Poisson arrivals at `--rate` per second |
| `bursty` | 10 % of tasks trickled over the first 60 % of the window, the rest flooded into the last 40 % |
| `periodic` | Poisson thinning with a sine-modulated acceptance probability (amplitude 0.7, three cycles per window) |

## Trace and results format

`generate` writes `workloads/<profile>-<pattern>-<seed>.jsonl`: UTF-8, LF endings, one JSON object
per line, sorted by offset. `timeout_ms` is written only when the task has a deadline, so a line
without it means "no deadline".

```json
{"offset_ms":828,"task_id":"mixed-42-00000","type":"FILE_IO_TASK","input":"size_mb=5, mode=write","priority":9}
{"offset_ms":1406,"task_id":"mixed-42-00001","type":"SLEEP_TASK","input":"ms=326","priority":7}
```

`replay` submits the trace with its original timing (`--speed` scales the clock) and writes one CSV
row per task to `results/<trace>-replay.csv`:

```
task_id,task_type,priority,offset_ms,submit_ms,start_ms,end_ms,latency_ms,status,worker,exec_ms
mixed-42-00000,FILE_IO_TASK,9,828,1568,60067,60067,58499,COMPLETED,worker-1,149
```

`start_ms` is the first poll that saw the task leave `QUEUED`, so `start_ms == end_ms` means the task
finished between two polls; `latency_ms` is `end_ms - submit_ms`, which includes the queue wait the
scheduler imposed. The first row above is the burst doing its job: submitted at 1.5 s, still waiting
at 60.1 s, because 270 other tasks were submitted in the same instant.

## How to run and demo

Three processes, then the two commands:

```bash
python scripts/mock-http.py --port 8100
java -jar predisched-scheduler/target/predisched-scheduler.jar --config configs/local.yaml
java -jar predisched-worker/target/predisched-worker.jar --config configs/local.yaml
java -jar predisched-client/target/predisched-client.jar workload generate --profile mixed --pattern bursty --tasks 300 --seed 42
java -jar predisched-client/target/predisched-client.jar workload replay workloads/mixed-bursty-42.jsonl
```

Other patterns and profiles: `--profile cpu_heavy --pattern steady --rate 20 --tasks 2000 --seed 7`.
`replay` takes `--speed 2.0` for a twice-as-fast run, `--output`, `--poll-ms` and `--timeout-ms`.

## Real output

Generator summary (acceptance check 1), the ten types in the `mixed` profile as configured:

```
wrote 300 tasks to workloads\mixed-bursty-42.jsonl (profile=mixed pattern=bursty seed=42 rate=5.0/s)
  COMPRESS_TASK: 35
  CPU_TASK: 29
  FILE_IO_TASK: 29
  GRAPH_TASK: 40
  HASH_TASK: 32
  HTTP_TASK: 25
  MATRIX_TASK: 29
  MONTE_CARLO_TASK: 27
  SLEEP_TASK: 27
  SORT_TASK: 27
```

Each type is 10 % of the mix; with 300 tasks the spread is sampling noise (25–40).

Determinism (rule 8): the same command run three times and once with a different seed, then sha256:

```
workloads/mixed-bursty-42.jsonl  4F1D736CB79316A163B3F246587B774F33E25EC65376BBEDCAEAD56D8613CC52
target/agent-tmp/run1.jsonl      4F1D736CB79316A163B3F246587B774F33E25EC65376BBEDCAEAD56D8613CC52
target/agent-tmp/run2.jsonl      4F1D736CB79316A163B3F246587B774F33E25EC65376BBEDCAEAD56D8613CC52
target/agent-tmp/run3.jsonl      EC50FC692555B1C03D23EA70F0A0679648B9C6AC14B2E60D53EB8CA4EE8141A4  (seed 43)
```

Replay of that trace against one scheduler and one worker (acceptance check 2):

```
replay summary:
  CPU_TASK: completed=29 failed=0 mean_exec_ms=22.0
  MATRIX_TASK: completed=29 failed=0 mean_exec_ms=42.1
  SLEEP_TASK: completed=27 failed=0 mean_exec_ms=375.0
  SORT_TASK: completed=27 failed=0 mean_exec_ms=66.6
  HASH_TASK: completed=32 failed=0 mean_exec_ms=69.9
  COMPRESS_TASK: completed=35 failed=0 mean_exec_ms=417.0
  MONTE_CARLO_TASK: completed=27 failed=0 mean_exec_ms=318.7
  FILE_IO_TASK: completed=29 failed=0 mean_exec_ms=100.3
  HTTP_TASK: completed=25 failed=0 mean_exec_ms=188.6
  GRAPH_TASK: completed=40 failed=0 mean_exec_ms=102.7
overall: completed=300 failed=0
results written to results\mixed-bursty-42-replay.csv
```

All 300 tasks completed, mean wait in the queue 15100 ms against a mean execution time of 169.8 ms,
makespan 60954 ms for a 60 s trace: with a pool of 4 on 2 physical cores, 300 tasks arriving at one
instant is a queueing problem, not a scheduling-strategy problem, and that is the point of a burst.

One task of each new type, submitted individually against the same running cluster:

```
accepted=true task_id=task-e410aae8 trace=861c6f92 lamport=5585 message='queued'
task_id=task-e410aae8 status=COMPLETED worker=worker-1 exec_ms=20 result='rounds=20000 digest=306487bcf0979b8404d249780e61f66f2e0d6ae865cd9ab9f316c54b244702cc'
  SORT_TASK n=500000, type=reversed   -> sorted=true sum=125000250000 first=1 last=500000 (2 ms)
  COMPRESS_TASK size_mb=4, level=6    -> raw_bytes=4194304 compressed_bytes=4195602 ratio=1.0003 sha256=31a9d416... (389 ms)
  GRAPH_TASK nodes=20000, algo=pagerank, seed=7 -> rank_sum=1.000000 top_node=11619 top_rank=0.000132 (10 ms)
  FILE_IO_TASK size_mb=4, mode=both   -> written_bytes=4194304 read_bytes=4194304 sha256=3883f99e... (113 ms)
  HTTP_TASK url=http://localhost:8100/delay?ms=120, timeout=5000 -> status=200 bytes=13 sha256=88003775... (127 ms)
  MONTE_CARLO_TASK samples=1000000, seed=11 -> inside=785201 pi_estimate=3.140804 (80 ms)
```

`COMPRESS_TASK ratio=1.0003` is honest, not a bug: the payload is a repeated seeded 64 KiB block, so
gzip already finds the repetition in the first block and there is nothing left to shrink.

Rejections, with the reason rather than a stack trace:

```
$ ... submit --type GRAPH_TASK --input "nodes=100, algo=dfs" --priority 5
accepted=false task_id=task-2afc66f5 trace=e87891df lamport=0 message='algo must be one of [bfs, pagerank] (got 'dfs')'
$ ... submit --type SORT_TASK --input "n=1" --priority 5
accepted=false task_id=task-5189a138 trace=68d51f27 lamport=0 message='n must be between 2 and 20000000 (got 1)'
$ ... submit --type ML_INFER_TASK --input model=x --priority 5
accepted=false task_id=task-c31f5fe6 trace=70dd93b1 lamport=0 message='type ML_INFER_TASK is not executable yet (arrives in prompt 16 (ML models)); supported types are [CPU_TASK, SLEEP_TASK, MATRIX_TASK, HASH_TASK, MONTE_CARLO_TASK, SORT_TASK, COMPRESS_TASK, GRAPH_TASK, FILE_IO_TASK, HTTP_TASK]'
```

Note the shell quoting: `--input` takes commas, so it must be quoted or the CLI parses `algo=dfs`
as a second argument.

## Tests

`ExecutorTest` (in `predisched-worker`) covers one executor per type: good input parses, bad input is
refused with the reason, and the same input twice gives the same output. It also asserts that
`FILE_IO_TASK` leaves an empty temp dir both after a normal run and after a cancellation that lands
mid-write, that `MATRIX_TASK` gives the same checksum with 1 and 4 threads, that the four reserved
types have no executor, and that `HTTP_TASK` fails on a 500.

`WorkloadGeneratorTest` (in `predisched-client`): the same seed gives a byte-identical trace file, a
different seed does not, mix proportions stay within ±5 % of config over 5,000 tasks, arrival offsets
are sorted and shaped like their pattern, the priority and timeout distributions are honoured, and
every generated input validates against `TaskInputSpec`.

`ReplayTest`: replaying a 50-task trace against an in-process scheduler and worker completes all 50
and writes the CSV, and bad arguments are refused.

## Known limits, resolved later

- One scheduler and one worker in the acceptance run, so the numbers above are single-node
  behaviour; prompt 06 starts a real cluster and prompt 08 puts strategies behind the dispatcher.
- `replay` polls every 100 ms, so `start_ms` and `end_ms` are accurate to about one poll interval.
  A failed submit (queue full) is recorded as `FAILED` with no worker and no execution time, which is
  indistinguishable in the CSV from a task that ran and failed; the trace id in the scheduler log
  separates them.
- Arrival patterns cover what spec §12.1 asks for, but nothing models a rate that grows over the run
  or a trace with correlated task sizes.
- `COMPRESS_TASK` and `SORT_TASK` use fixed internal seeds, so their cost is not tunable beyond
  their size knobs; `MONTE_CARLO_TASK` and `GRAPH_TASK` take a seed and are therefore tunable.
