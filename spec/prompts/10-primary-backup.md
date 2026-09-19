# Prompt 10: Primary-backup fault tolerance and worker failover (Exp 8)

## Goal

The elected leader is the primary scheduler, and every state change reaches the backups before the
client is acknowledged. Killing the primary mid-run promotes a backup through election, and it resumes
dispatch with no task lost or duplicated. Tasks on a dead worker return to the queue. A recovered node
rejoins as a backup and catches up. This is lab Exp 8.

## Read first

- Spec §4 FR12, §5 (Availability, Reliability), §11 Exp 8, §16 (Fault demos), §17 (Primary-backup,
  Fault injection rows)

## Build

1. **`predisched-fault`**
   - `PrimaryBackupCoordinator`: listens to leader changes from `predisched-election`. On becoming
     primary it (a) checks its replication log is at the highest sequence number among reachable
     backups and pulls any gap with `SyncFrom`, (b) rebuilds the queue from the store (every `QUEUED`
     task, every `BLOCKED` task with its parents, every `RUNNING` task becomes an in-doubt dispatch),
     (c) starts the dispatcher. On losing primacy it stops dispatching.
   - Write path: the primary uses the replication layer from prompt 07 in a synchronous
     primary-backup mode: log with a sequence number, forward to all live backups, wait for their acks
     (backups apply strictly in sequence order and buffer out-of-order messages), then ack the client.
     A backup that misses acks is dropped from the live set and must catch up with `SyncFrom` before
     it rejoins.
   - In-doubt dispatches: a task `RUNNING` on a worker when the primary died is resolved by asking
     that worker (`WorkerService.QueryExecution`, add to `worker.proto`): still running → re-attach,
     finished → take its result, unknown → re-queue. Task ids plus a per-attempt dispatch id make
     execution idempotent, so a worker never runs the same attempt twice.
   - `WorkerFailureDetector`: after `worker.heartbeat.misses` (default 3) missed heartbeats the worker
     is marked `DEAD`, and its `RUNNING` tasks go back to `QUEUED` with reason `WORKER_LOST`. A worker
     that comes back re-registers as new.
   - Workers re-register with the new primary when their heartbeat is answered by a follower (use the
     `leader=<id>` hint from prompt 06).
2. **Client failover**: `SchedulerClient` holds all scheduler addresses, follows `leader=<id>` hints,
   and retries with backoff when the primary is unreachable. Submits carry the client-chosen task id,
   so a retried submit is idempotent.
3. **`predisched-benchmark`**: `FailoverTest` command. It submits 100 tasks, kills the primary process
   at task 50 (real process kill via the cluster scripts, not a flag), waits for completion, then
   checks every one of the 100 ids is `COMPLETED` exactly once. It also records the time from kill to
   first dispatch by the new primary. Writes `results/exp8-failover.csv`.
4. **Scripts**: `scripts/kill-primary` (asks `cluster leader`, kills that process) and
   `scripts/restart-node`.

## Tests

- Backup applies out-of-order messages in sequence order.
- In-process 3-node test: kill the primary between the backup ack and the client ack; the client
  retry is idempotent and the task exists exactly once.
- In-doubt resolution: all three cases (still running, finished, unknown).
- Worker death: 3 missed heartbeats re-queue its running tasks, and they complete elsewhere.
- Rejoin: a restarted node catches up with `SyncFrom` before joining the live set.

## Acceptance checks

```bash
scripts/start-cluster.sh
```
```bash
java -jar predisched-benchmark/target/predisched-benchmark.jar failover-test --tasks 100 --kill-at 50
```
```bash
scripts/stop-node.sh worker 2
```

Paste the failover summary (100/100 completed, 0 lost, 0 duplicated, failover time), the election
and promotion log lines, and the reassignment log lines after the worker kill.

## Docs and commit

- `docs/components/fault-tolerance.md`: write path diagram, promotion steps, in-doubt resolution,
  worker failure detection, measured failover time.
- `docs/LAB-COVERAGE.md` Exp 8 row.
- Commit: `Fault tolerance: primary-backup failover and worker task reassignment with no task loss`
