# Fault tolerance: primary-backup failover and worker reassignment (lab Exp 8)

The elected leader is the **primary** scheduler; the other schedulers are **backups**. Every change
to task state is logged with a sequence number and reaches every live backup before the client is
acknowledged. When the primary dies, election promotes a backup, which already holds the task state,
settles whatever the old primary left running, and resumes dispatch with no task lost or duplicated.
A worker that misses three heartbeats is declared dead and its running tasks go back to the queue. A
node that comes back rejoins as a backup and catches up before it counts.

Built by `spec/prompts/10-primary-backup.md`. Spec sections: §4 FR12, §5 (Availability,
Reliability), §11 Exp 8, §16 (Fault demos), §17 (Primary-backup, Fault injection rows).

## Pieces

| Where | Class | Role |
| --- | --- | --- |
| `predisched-fault` | `PrimaryBackupCoordinator` | Listens to leader changes; promotes this node in three steps, or demotes it; runs a backup's rejoin loop. |
| | `InDoubtResolver`, `InDoubtActions` | Settles each task the old primary left RUNNING by asking its worker. |
| | `WorkerFailureDetector` | Declares a worker DEAD after `worker.heartbeatMisses` (3) silent heartbeat intervals. |
| | `Recovery`, `PrimaryRole` | The store sorted by what a new primary must do; the scheduler's side of promotion. |
| `predisched-replication` | `PrimaryBackupReplication` | Consistency mode `primary-backup`: numbered writes, synchronous fan-out to the live set, in-order apply with a buffer, `Join`, catch-up, fork detection. |
| `predisched-scheduler` | `SchedulerFailover` | Implements `PrimaryRole`, `InDoubtActions` and the detector's `Workers`: rebuilds queue, workflow edges, dead letters and quotas; re-attaches; re-queues a dead worker's tasks. |
| | `Dispatcher` | Sends a dispatch id with every call; `reattach`, `takeResult`; suspects a worker whose call fails UNAVAILABLE. |
| | `RunningTasks` | An attempt is settled exactly once, by the dispatcher or the failure detector, whoever comes first. |
| `predisched-worker` | `ExecutionEngine`, `WorkerServiceImpl` | Runs each dispatch id at most once; `QueryExecution` reports RUNNING, FINISHED (with the result) or UNKNOWN. |
| `predisched-client` | `SchedulerClient` | Holds every scheduler address, follows `leader=<id>` hints, retries with backoff. |
| `predisched-election` | `AbstractElection` | A node that starts while a leader serves adopts it instead of calling an election. |

Contracts: `worker.proto` gains `ExecuteRequest.dispatch_id` and `QueryExecution`;
`replication.proto` gains `Join`; `task.proto` gains `TaskRecordProto.depends_on` (18) so BLOCKED
workflow tasks can be rebuilt on a new primary.

## Write path

```
client          primary (node 5)                       backups 1..4 (live set)
  |  SubmitTask       |                                        |
  |------------------>| store.put: seq = last + 1              |
  |                   | apply + log locally                    |
  |                   |---- Replicate(seq, record) ----------->| apply in seq order
  |                   |     (all live backups in parallel)     | (buffer if seq arrives early,
  |                   |<--------------------------- ack -------|  ack only once applied)
  |                   | every ack in, or a backup timed out    |
  |                   |   -> that backup dropped from live set |
  |<---- accepted ----|                                        |
```

- Sequence numbers are assigned under one lock with the local apply, so the primary's log order is
  the write order. Forwarding happens outside the lock, from many dispatch threads at once, so a
  backup may receive seq 7 before seq 6: it buffers 7 and holds its ack until 6 is applied. An ack
  therefore means "this change and every one before it".
- A backup that does not ack within `replication.writeTimeoutMs` (1 s) is dropped from the live set,
  so one dead backup costs one slow write, not every write.
- A backup (re)joins by catching up with `SyncFrom`, then calling `Join(last_seq, last change)`. The
  primary checks under its sequence lock that the backup's last seq equals its own and that the
  change there is the same one, then adds it. No write can fall between the catch-up and the join.
- If `Join` or catch-up shows the backup's log **forked** from the primary's (it kept writes a demoted
  primary made, for example), the backup drops its log and copies the primary's from seq 1.

## Promotion

`PrimaryBackupCoordinator` handles leader changes one at a time on its own thread. On winning:

1. **Log complete.** Stop accepting joins (`becomePrimary`), then pull from every reachable peer
   whatever it holds beyond this node's last seq (`SyncFrom`). Every backup's log is a prefix of the
   dead primary's, so the longest reachable one is the most complete. Then start accepting joins.
   The ordering matters: a backup must not compare itself against a new primary whose log is still
   short, or it would take its own longer log for a fork and drop it.
2. **Queue rebuilt from the store.** Every QUEUED task is queued. Every workflow task is re-linked to
   its parents from `depends_on`: a BLOCKED task whose parents all finished is released, one with a
   failed parent is cancelled. FAILED tasks go back into the dead-letter queue, client quotas are
   recounted, and every RUNNING task is an **in-doubt dispatch** for the resolver.
3. **Dispatch.** Start the dispatcher and the worker failure detector. Only now does the node accept
   submits; until then it answers `not the leader; leader=<self>` and clients wait and retry.

On losing the leadership it stops dispatching and clears its live set at once, so a stale write
reaches no backup.

## In-doubt dispatches

An attempt is named by its dispatch id, `<taskId>#<attempt>`, where attempt is the record's attempt
count plus one. The old primary used the same rule, so both name the same attempt without storing
anything. The new primary asks the task's worker (`QueryExecution`):

| Worker says | Action |
| --- | --- |
| RUNNING | Re-attach: send `ExecuteTask` with the same dispatch id. The worker sees the id and the new call waits for the run in progress. The outcome is recorded as usual. |
| FINISHED | Take the result it kept: record that attempt's outcome now. |
| UNKNOWN | It never arrived (the primary died between marking it RUNNING and calling): back to QUEUED, no attempt used. |
| (no answer) | The worker is gone too: the attempt ends WORKER_LOST and the retry rules decide. |

The worker keeps the outcomes of its last 10 000 dispatches. A worker never runs one dispatch id
twice, which is what makes both re-attaching and a client's retry safe.

## Worker failure

- **Detector.** On the primary, every half heartbeat interval: a worker silent for
  `heartbeatMisses` × `heartbeatIntervalMs` (3 × 1 s) is removed from the registry and its channel
  closed. Every attempt still running on it is settled as WORKER_LOST and handed to the retry rules,
  which re-queue it after the backoff, avoiding the worker it was lost on. A worker that comes back
  finds itself unknown at its next heartbeat and registers as new.
- **Faster path.** A killed worker process resets its connections, so the call in flight fails
  UNAVAILABLE at once. The dispatcher records WORKER_LOST and **suspects** the worker: no dispatch
  goes to it until it heartbeats again. The detector still declares it dead later.
- **Chosen, then dead.** The dispatcher picks a worker when it hands a task to its dispatch pool. If
  that worker has been dropped or suspected by the time the call would go out, the task goes back to
  the queue without using an attempt.

`RunningTasks` settles each attempt exactly once. Whoever settles it first (the dispatcher when its
call returns, or the detector) records the outcome; a late reply from a worker already given up on
is ignored.

## Client failover

`SchedulerClient.forCluster` (the CLI uses it whenever `--config` lists `election.peers`) keeps a
channel to every scheduler.

- A refusal naming a leader redirects the call at once.
- When the target stops answering, the client pings every scheduler, sends the call to the leader
  most of them name, and backs off (100 ms doubling to 2 s) while no leader is known yet.
- Submits carry the client-chosen task id. A submit whose reply was lost is retried, and the
  scheduler answers `already accepted (<status>)` if the stored task is the same one (type, input,
  priority, client). A *different* task under a taken id is still refused (FR2).

## Rejoining

A restarted scheduler starts empty (state is in memory until prompt 11) and rejoins as a backup:

- **Election.** On start, a node asks its peers who leads. If a leader is serving, the node adopts it
  instead of calling an election. Classic Bully would let the restarted highest id take over again;
  here it would unseat a working primary for nothing.
- **Replication.** Its rejoin loop (every `catchUpIntervalMs`, 500 ms) catches up from the primary
  with `SyncFrom` from seq 1 and joins the live set.

Workers register with every scheduler (prompt 06), so the restarted node learns them within one
heartbeat.

## Design choices and limits

- **Channels to restarted nodes.** gRPC backs off reconnecting to a peer that failed, up to two
  minutes, and fails calls at once meanwhile. The first live run exposed this: after a restart, a
  Bully `Election` message to the restarted node 5 failed instantly, node 3 elected itself while 5
  did too, and 13 tasks were started twice. `Transport.reconnecting` now skips the backoff of a
  failed channel before every node-to-node call (election, replication, worker, registration,
  client). A peer still down refuses the new attempt just as fast.
- **Workers and the `leader=<id>` hint.** The prompt asks workers to re-register with the new
  primary by following a follower's `leader=<id>` hint. Here workers already register with, and
  heartbeat, every scheduler (prompt 06), so the new primary knows every live worker when it is
  promoted. No hint is needed, and there is no re-registration round in the failover time.
- **No fencing.** Election plus primary-backup cannot stop two primaries under a real network
  partition: there are no terms or quorum leases (Raft is future work, spec §20). A demoted primary
  stops dispatching, its log is detected as forked when it rejoins, and replaced.
- **Right after a promotion the live set is empty.** Backups join within one catch-up interval
  (about 50–600 ms in the runs below). Writes in that window are acknowledged by the new primary
  alone.
- **Re-attached calls hold dispatch threads.** Calls to workers block one of the `dispatchThreads`
  (4) for the task's duration. After a failover with 4 tasks re-attached, the first *new* dispatch
  waits for one of them to finish. That is why the 6 s run below resumes in 1.6 s but first
  dispatches a new task after 5 s.

## How to run and demo

```bash
mvn -q verify
scripts/start-cluster.sh                     # Windows: scripts\start-cluster.ps1
predisched --config configs/cluster.yaml cluster leader
java -jar predisched-benchmark/target/predisched-benchmark.jar failover-test --tasks 100 --kill-at 50
scripts/restart-node.sh scheduler 5          # the killed primary rejoins as a backup
java -jar predisched-benchmark/target/predisched-benchmark.jar failover-test --tasks 40 --kill-at 20 --input ms=6000
predisched --config configs/cluster.yaml submit --type SLEEP_TASK --input ms=8000 --priority 5 --repeat 8
scripts/stop-node.sh worker 2                # its tasks complete on the other workers
scripts/stop-node.sh all
```

`failover-test` kills the primary with `scripts/kill-primary.sh` (`.ps1` on Windows): it asks
`cluster leader` and kills that process. It then polls until every task is finished and checks:

- each task is COMPLETED with exactly one SUCCEEDED attempt;
- the workers' event logs show how many times each task started;
- from the new primary's event log: when it was elected, how long promotion took, and its first
  dispatch or re-attach after the kill.

Results are appended to `results/exp8-failover.csv`.

## Real output

Run on Windows 11, 5 schedulers and 3 workers from `configs/cluster.yaml`, all on one machine.

`failover-test --tasks 100 --kill-at 50` (tasks `SLEEP_TASK ms=2000`):

```
failover-test fo-0927-030945: 100 x SLEEP_TASK(ms=2000), primary is scheduler 5, kill after task 50
  task 50 submitted; killing the primary:
    | primary is scheduler 5: killing it
    | stopped scheduler-5 (pid 25952)
  100/100 accepted; waiting for them to finish

Failover summary (fo-0927-030945)
  primary killed:     scheduler 5, after task 50 of 100
  new primary:        scheduler 4 (leader 1003 ms after the kill, promotion 218 ms)
  failover time:      1137 ms from the kill to the new primary's first dispatch or re-attach
  first new dispatch: 1826 ms after the kill
  completed:          100/100
  lost:               0
  duplicated:         0 (tasks started more than once on workers: 0; seen starting: 100)
```

Election and promotion on scheduler 4:

```
03:09:58.265 WARN  [scheduler-4 L=2825 ] AbstractElection - Leader 5 stopped answering; node 4 starts a bully election
03:09:58.295 INFO  [scheduler-4 L=2826 ] AbstractElection - Leader is now 4 (bully): elected in 21 ms
03:09:58.316 INFO  [scheduler-4 L=2831 ] PrimaryBackupCoordinator - Node 4 won the election: promoting to primary
03:09:58.356 INFO  [scheduler-4 L=2831 ] PrimaryBackupCoordinator - Promotion 1/3: replication log at seq 86 (was 86); unreachable or skipped: [5]
03:09:58.386 INFO  [scheduler-4 L=2831 ] SchedulerFailover - Queue rebuilt from the store: 30 queued, 0 in the dead-letter queue
03:09:58.386 INFO  [scheduler-4 L=2831 ] PrimaryBackupCoordinator - Promotion 2/3: rebuilt from the store: 30 QUEUED, 0 BLOCKED, 4 RUNNING in doubt, 16 finished
03:09:58.438 INFO  [scheduler-4 L=2831 ] InDoubtResolver - In-doubt fo-0927-030945-020 on worker-2 (dispatch fo-0927-030945-020#1): still running there, re-attached
03:09:58.452 INFO  [scheduler-4 L=2864 ] PrimaryBackupReplication - Backup 3 caught up to seq 86: joined the live set, now [3]
03:09:58.486 INFO  [scheduler-4 L=2856 c1e930de] Dispatcher - Re-attaching to fo-0927-030945-020 on worker-2 (dispatch fo-0927-030945-020#1)
03:09:58.534 INFO  [scheduler-4 L=2831 ] PrimaryBackupCoordinator - Promotion 2/3: in-doubt dispatches settled: {REATTACHED=4}
03:09:58.534 INFO  [scheduler-4 L=2831 ] PrimaryBackupCoordinator - Promotion 3/3: node 4 is the primary and dispatching, 218 ms after the leader change
03:09:58.872 INFO  [scheduler-4 L=2929 ] PrimaryBackupReplication - Backup 1 caught up to seq 86: joined the live set, now [1, 3]
03:09:58.875 INFO  [scheduler-4 L=2931 ] PrimaryBackupReplication - Backup 2 caught up to seq 86: joined the live set, now [1, 2, 3]
```

On the worker, the re-attached call joins the run in progress:

```
Dispatch fo-0927-025440-005#1 is already running here: the new call waits for that run
```

`scripts/restart-node.ps1 scheduler 5`: the old primary rejoins as a backup and catches up.

```
03:11:11.397 INFO  [scheduler-5 L=0 ] AbstractElection - Node 5 found leader 4 already serving: rejoins as a follower
03:11:11.835 INFO  [scheduler-5 L=18014 ] PrimaryBackupReplication - Caught up from node 4: applied 300 changes, seq 0 -> 300
03:11:11.851 INFO  [scheduler-5 L=18014 ] PrimaryBackupCoordinator - Node 5 is a live backup of primary 4 at seq 300
03:11:11.835 INFO  [scheduler-4 L=18339 ] PrimaryBackupReplication - Backup 5 caught up to seq 300: joined the live set, now [1, 2, 3, 5]
```

Then `failover-test --tasks 40 --kill-at 20 --input ms=6000` kills scheduler 4. Scheduler 5 is
promoted, re-attaches 4 running tasks, and 40/40 complete once each (0 started twice on workers).

`scripts/stop-node.ps1 worker 2` while 8 tasks of 8 s run, on primary scheduler 5:

```
03:12:43.943 WARN  [scheduler-5 L=31032 0c2834e5] Dispatcher - Worker worker-2 call failed for task-1be8f865: UNAVAILABLE: io exception
03:12:43.954 INFO  [scheduler-5 L=31032 0c2834e5] RetryCoordinator - Attempt 1 of task-1be8f865 failed (UNAVAILABLE: io exception); retrying in 218 ms (1 of 3 retries used)
03:12:46.671 WARN  [scheduler-5 L=31479 ] SchedulerFailover - Worker worker-2 missed 3 heartbeats (3201 ms silent): marked DEAD; re-queueing its 0 running tasks
03:12:48.689 INFO  [scheduler-5 L=31590 ] Dispatcher - Worker worker-2 went away before task-eebd533d was sent to it; re-queueing
03:12:48.772 INFO  [scheduler-5 L=31642 0c2834e5] Dispatcher - Dispatching task-1be8f865 to worker-1 (attempt 2)
```

All 8 completed: `task-1be8f865` on worker-1 at attempt 2, `task-eebd533d` on worker-3 at attempt 1,
the rest on their first worker. The detector re-queued nothing here because the process kill had
already failed the running call. The detector path, a worker that hangs rather than dies, is
covered by `WorkerDeathTest`.

## Measured failover time

`results/exp8-failover.csv`, one machine, Bully, ping every 500 ms with 3 misses:

| Run | Tasks | Killed → new | Elected after kill | Promotion | Resumed (dispatch or re-attach) | First new dispatch | Completed | Lost | Duplicated |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fo-0927-030945 | 100 × 2 s, kill at 50 | 5 → 4 | 1003 ms | 218 ms | 1137 ms | 1826 ms | 100/100 | 0 | 0 |
| fo-0927-031119 | 40 × 6 s, kill at 20 | 4 → 5 (restarted) | 1413 ms | 249 ms | 1635 ms | 5018 ms | 40/40 | 0 | 0 |

Failover is dominated by failure detection: three missed 500 ms pings is about 1–1.5 s. The Bully
election itself takes 20–30 ms and promotion about 0.2 s, including querying the workers about the 4
in-doubt tasks.

## Tests

- `PrimaryBackupTest`:
  - seq 3 and 2 arriving before 1 are held, then applied as 1, 2, 3, and their acks complete only then;
  - a write is on both backups the moment it returns;
  - a killed backup is dropped, and restarted empty it is refused a join until it catches up with
    `SyncFrom`, then gets writes synchronously;
  - a forked backup drops its log and copies the primary's;
  - a new primary refuses joins until step 1 is done.
- `FailoverIntegrationTest`: three in-process schedulers with a fake worker. The primary is
  crashed after the backups acked a submit but before the client's reply. The client's retry
  reaches the new primary and gets `already accepted`. All 10 tasks complete exactly once, and the
  worker ran each once.
- `InDoubtResolverTest`: RUNNING re-attaches, FINISHED takes the result, UNKNOWN re-queues; the
  dispatch id names the right attempt; an unreachable worker is asked only once.
- `WorkerDeathTest`:
  - two missed heartbeats are not enough, the third is;
  - the hung worker's two tasks complete on the other worker (WORKER_LOST then SUCCEEDED);
  - a task chosen for a worker that then went away is re-queued without an attempt.
- `WorkerFailureDetectorTest`: the miss arithmetic.
- `DispatchIdempotencyTest`: a repeated dispatch id attaches to the running run, and after it
  finishes gets the recorded result; one execution. `QueryExecution` reports all three states.
- `PromotionRebuildTest`: step 2 over a store left as a dying primary would leave it.
- `SchedulerClientFailoverTest`: redirect by hint; primary gone, retried until the new one answers.
- `ElectionScenarios` (Bully and Ring): a restarted node rejoins as a follower.
- `SchedulerIntegrationTest`: resubmitting the same task is idempotent; a different task under a
  taken id is refused.
