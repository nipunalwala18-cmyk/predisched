# Speculation and chaos

Prompt 19, F11, F16, FR28, FR35, spec §15.7. This prompt adds two things:

- **Speculative execution.** A straggling task gets a duplicate on another worker; the first
  success wins and the loser is cancelled.
- **Chaos API.** Every node can be made slow, busy, partitioned, drained or crashed on demand.

## Speculative execution (F11)

**Straggler detection.** `StragglerDetector` runs every `speculation.checkMs` (250 ms in
`configs/chaos.yaml`) over the running original attempts. An attempt is a straggler once its
elapsed time exceeds:

```
threshold = max(k × predicted_exec_ms, p90 exec time of its task type)
```

- With no prediction, the p90 alone applies. The prediction exists only under the predictive
  strategy, through the new `SchedulingStrategy.predictedExecMs` hook.
- With no p90 yet, k × prediction applies alone. The p90 comes from the last `window` (200)
  completed runs of the type, and needs `minSamples` (20) of them.
- With neither, the task is left alone.
- k is 2 by default.

**Launching the duplicate.** `Dispatcher.speculate()` handles this:

- The active strategy places the copy, with the straggler's worker removed from the candidates.
- A copy is launched only if some other worker has spare capacity (the usual outstanding limit).
- There is at most one copy per task.
- The copy is attempt n+1 with its own dispatch id. `RunningTasks` now keeps it beside the
  original under its own key, so both count toward capacity and both can time out.

**The race.** Both copies' replies go through `settleRace`:

- **The first success wins.** A compare-and-set on the race decides it. The winner then settles
  the other copy in `RunningTasks`, so the other's late reply is ignored as already settled. It
  sends `CancelExecution` to the other copy's worker and records that attempt as
  `SPECULATIVE_LOSER`, which does not count against retries. Then the winner takes the normal
  success path. If the copy won, the task's worker becomes the copy's worker.
- **A copy that fails while the other still runs** is recorded as a failed attempt and ends the
  race. The survivor then finishes on the normal path, with retries if it fails too.
- **A worker declared dead** (prompt 10) that held one copy does not re-queue the task while the
  other copy runs.

**Counters.** Launched, won by the copy, won by the original, and wasted work: the time the loser
had run when cancelled. They are logged with every race result, and the `SPECULATE` and
`SPECULATION_RESULT` events go to the event log.

Speculation is **off by default** (`speculation.enabled: false`), so earlier experiments stay
reproducible. `configs/chaos.yaml` turns it on.

## Chaos API (F16)

`proto/chaos.proto` defines `ChaosService`. Every scheduler and worker serves it, only when
`chaos.enabled: true`:

| RPC | Effect | Ends |
| --- | --- | --- |
| `InjectLatency(ms, duration_s)` | `ChaosInterceptor` sleeps `ms` before every incoming call, except ChaosService calls | after the duration |
| `SpikeCpu(threads, duration_s)` | busy-looping threads (0 = one per core) | the threads stop at the end time |
| `Isolate(duration_s)` | replication traffic refused both ways (below) | after the duration |
| `Drain()` | worker: running and queued tasks finish, new ones are refused | when the worker restarts |
| `Crash(reason)` | replies, logs `CHAOS: <node> crashing on request`, then halts the process (exit 137) | |

**Isolation.** Incoming `ReplicationService` calls get `UNAVAILABLE`, and a client interceptor
fails the node's *own* outgoing replication calls. The outgoing interceptor is installed through a
new process-wide `Transport.addOutgoing`, which even channels built earlier consult at call time.

The first version blocked only incoming calls. In the live test the isolated backup was dropped by
the primary and then pulled a catch-up (`SyncFrom`) and rejoined 0.2 s later, mid-partition. With
both directions cut, it rejoined 0.27 s after the partition ended (see below).

**Safeguards.**

- Durations are capped at 1 h and latency at 60 s; bad values get `INVALID_ARGUMENT`.
- Every effect carries its own end time, so nothing has to switch it off.
- Every action writes a `CHAOS` event on the node it hits, stamped with that node's Lamport time.

**Draining.** Workers report `draining` in their heartbeats (`Heartbeat.draining`), and the
scheduler's `WorkerRegistry` keeps them out of the candidate list. A task that reaches a draining
worker anyway is refused like a full queue, so it is re-queued without spending a retry.

**CLI** (`ChaosCommand`):

```
predisched chaos kill-worker <id>
predisched chaos kill-primary
predisched chaos latency <node> <ms> <sec>
predisched chaos cpu <node> <sec> [--threads n]
predisched chaos partition <node> <sec>
predisched chaos drain <worker>
predisched chaos burst <n> [--profile bursty] [--seed s]
```

- A worker id is resolved through the scheduler's new `ListWorkers` RPC.
- `scheduler-N` is resolved through the election peers.
- `kill-primary` asks the cluster who leads (`locateLeader`) and crashes that node.
- `burst` submits n generated tasks at once, and writes its `CHAOS` event to the client's own
  event log (`logs/client-chaos.jsonl`).

## Acceptance run

`scripts/start-cluster.ps1 -Config configs/chaos.yaml` starts:

- 5 schedulers, with primary-backup replication and Bully election;
- 3 workers, each with a pool of 4;
- least-loaded placement, with speculation and chaos on.

Scheduler 5 leads.

**1. Latency on worker-2, then a long-tail replay:**

```
$ predisched --config configs/chaos.yaml chaos latency worker-2 1500 60
worker-2 (localhost:51062): +1500 ms on every call for 60 s, until 2026-09-27T08:49:08.834Z
$ predisched --config configs/chaos.yaml workload replay workloads/campaign/long_tail-periodic-1514.jsonl --id-suffix "~p19"
...
overall: completed=160 failed=0
```

The speculation counters at the end of the replay (primary's log):

```
speculations launched=9, won by the copy=4, won by the original=5, wasted work=2957 ms
```

One straggler, from dispatch to result:

```
14:18:20.265 Dispatching long_tail-1514-00062~p19 to worker-2 (attempt 1)
14:18:20.943 Straggler long_tail-1514-00062~p19 has run 679 ms on worker-2 (attempt 1); speculative copy as attempt 2 on worker-1
14:18:21.431 Dispatching long_tail-1514-00062~p19 to worker-1 (attempt 2)
14:18:23.020 Speculation for long_tail-1514-00062~p19: worker-1 won (speculative copy), worker-2 cancelled; speculations launched=1, won by the copy=1, won by the original=0, wasted work=1227 ms

$ predisched --config configs/chaos.yaml status "long_tail-1514-00062~p19"
task_id=long_tail-1514-00062~p19 status=COMPLETED worker=worker-1 exec_ms=56 trace=2c4b8861 attempts=2 result='slept_ms=55'
  attempt #1 SPECULATIVE_LOSER on worker-2 (lost the race to worker-1) 0ms
  attempt #2 SUCCEEDED on worker-1 56ms
```

A losing copy cancelled on its worker: `worker-1 ... ExecutionEngine - Cancelled
long_tail-1514-00092~p19 (speculative loser), stopped=true`.

**What the counters show.** Only 9 of worker-2's tasks were raced, and the original won 5 of
them. Three things limit it:

- **The p90 needs history.** It only exists once 20 runs of a type have completed, so worker-2's
  early tasks were never raced.
- **Least loaded stops feeding worker-2.** Its calls held their in-flight slots for 1.5 s longer,
  so the strategy sent it less and less work.
- **Copies often queued.** They went to the busier fast workers, so a 1.5 s-delayed original could
  still finish first.

There is also a lag between deciding to speculate and the copy's dispatch log line (about 0.5 s
above), because the dispatch pool's threads were blocked on worker-2's delayed calls. Even so, all
160 tasks completed exactly once.

**2. `kill-primary`:**

```
$ predisched --config configs/chaos.yaml chaos kill-primary
primary is scheduler-5 (localhost:51055)
scheduler-5 (localhost:51055): crashing
```

```
14:19:04.142 [scheduler-5] ChaosController - CHAOS crash on scheduler-5 {reason=chaos kill-primary}
14:19:04.348 [scheduler-5] ChaosController - CHAOS: scheduler-5 crashing on request (chaos kill-primary)
14:19:05.918 [scheduler-4] AbstractElection - Leader 5 stopped answering; node 4 starts a bully election
14:19:05.931 [scheduler-4] AbstractElection - Leader is now 4 (bully): elected in 12 ms; sent by this node since start: election=2 ok=5 coordinator=0 ring_pass=0 ping=160
14:19:05.940 [scheduler-4] SchedulerNode - This scheduler (node 4) is now the leader
14:19:05.940 [scheduler-4] PrimaryBackupCoordinator - Node 4 won the election: promoting to primary
14:19:05.954 [scheduler-4] PrimaryBackupCoordinator - Promotion 1/3: replication log at seq 490 (was 490); unreachable or skipped: [5]
14:19:05.974 [scheduler-4] PrimaryBackupCoordinator - Promotion 2/3: rebuilt from the store: 0 QUEUED, 0 BLOCKED, 0 RUNNING in doubt, 160 finished
14:19:05.980 [scheduler-4] PrimaryBackupCoordinator - Promotion 3/3: node 4 is the primary and dispatching, 40 ms after the leader change
14:19:06.430 [scheduler-4] PrimaryBackupReplication - Backup 2 caught up to seq 490: joined the live set, now [1, 2, 3]
```

`cluster leader` afterwards: schedulers 1–4 name leader 4, and scheduler 5 is unreachable.

**3. The other actions:**

```
$ predisched --config configs/chaos.yaml chaos drain worker-3
worker-3 (localhost:51063): draining: running tasks finish, new ones are refused
$ predisched --config configs/chaos.yaml chaos burst 30 --profile bursty --seed 19
burst: 30 of 30 bursty tasks accepted in 910 ms (seed 19)
$ predisched --config configs/chaos.yaml chaos cpu worker-1 5 --threads 2
worker-1 (localhost:51061): CPU spike for 5 s, until 2026-09-27T08:49:36.137Z
```

- **Drain.** The primary logged `Worker worker-3 is draining: no new dispatches to it`. The burst's
  30 tasks went 6 to worker-1 and 24 to worker-2, none to worker-3.
- **Partition.** On a fresh cluster, `chaos partition scheduler-2 8` ran with a 5-task burst during
  it. The primary logged `Backup 2 did not ack seq 1 within 1000 ms: dropped from the live set` at
  14:23:55.300. Backup 2 rejoined at 14:24:01.531 (`Backup 2 caught up to seq 15`), just after the
  partition's end at 14:24:01.262.

## Tests

- `SpeculationTest`:
  - the threshold with and without a prediction and with no history;
  - the type p90's sample minimum and window;
  - a long-tail workload of 40 tasks (20–60 ms plus some 150 ms) on three fake workers, one 25x
    slower. Speculations are launched and won by the copy, the slow worker's losing copies get
    `CancelExecution`, and every task ends COMPLETED with exactly one SUCCEEDED attempt. There is
    at least one `SPECULATIVE_LOSER`, wasted work > 0, and nothing left in flight. It passed 5 of
    5 runs in about 1.7 s.
  - a heartbeat with `draining` removes the worker from the candidates, and one without it brings
    the worker back.
- `ChaosTest` (in-process node, fake clock):
  - latency delays calls while active, never delays ChaosService, and ends after its duration;
  - isolation refuses incoming replication only and ends;
  - the outgoing interceptor refuses the node's own replication calls only while isolated;
  - the CPU spike's threads run and stop at the end time;
  - drain runs the node's drain, and a scheduler has nothing to drain;
  - crash replies first, then exits with 137;
  - bad durations get `INVALID_ARGUMENT`;
  - each action writes one `CHAOS` event with a Lamport time.
- `ExecutionEngineTest.drainingFinishesRunningWorkAndRefusesNewTasks`: after `drain()`, the
  running and the queued task both finish, and a new one is refused as rejected.
