# Prompt 07 — Fault tolerance: primary-backup failover and worker failure recovery

**Spec sections:** §4 (FR12), §5 (Availability, Reliability), §9 "Exp 8", §15 (Primary-backup, Fault injection)
**Depends on:** 06
**Lab topics:** Fault tolerance with primary-backup replication
**Commit message:** `Fault tolerance: primary-backup schedulers fail over with no task loss; dead workers' tasks reassigned`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Make the product survive the loss of its primary
scheduler or of any worker without losing or duplicating a single accepted task.

### Primary-backup (`predisched-fault`)

- The elected leader is the **primary**; the other schedulers are **backups**. Replication runs in
  strong mode for primary-backup regardless of the general setting, and the write path is:
  client → primary assigns `seq_no` → `Replicate` to backups → backups apply **in `seq_no` order**
  (buffer out-of-order entries) and ack → primary acks the client only after the required acks.
- The queue itself is derived state: a backup rebuilds its `TaskQueue` from replicated QUEUED records,
  so promotion needs no extra transfer.
- `PromotionHandler` (listens to `onLeaderChanged`): the new primary
  1. finishes applying any buffered log entries,
  2. reconciles RUNNING tasks by asking each live worker which task ids it holds (new
     `WorkerService.ListTasks` RPC); RUNNING tasks no worker holds go back to QUEUED,
  3. rebuilds the queue and resumes dispatching,
  4. writes a FAILOVER event with the time from detection to resumed dispatch.
- `RejoinHandler`: a restarted scheduler joins as a backup, calls `SyncFrom(lastAppliedSeq)` against the
  primary, and only then counts toward quorum.
- **Exactly-once effect:** workers keep a bounded set of recently completed task ids with their
  results; a re-sent `ExecuteTask` for a completed id returns the cached result instead of running
  again. Results reported to a stale primary are forwarded to the current one.

### Worker failure

- `WorkerFailureDetector` on the primary: a worker missing `missedHeartbeats` (default 3) heartbeats is
  marked DEAD, removed from `alive()`, and each of its RUNNING tasks returns to QUEUED with
  `attempts+1`. Tasks exceeding `maxAttempts` become FAILED.
- A DEAD worker that heartbeats again re-registers as a fresh worker.
- Every detection and recovery writes a `FailureRecord(nodeId, type, detectedAt, recoveredAt)`.

### Fault injection (part of the product's operator tooling)

- `predisched admin kill <nodeId>` asks a node to exit abruptly (`Runtime.halt`), for demos and
  benchmarks. Disabled unless `settings.allowFaultInjection: true`.
- `predisched-benchmark failover-drill`: submits N tasks, kills the primary after K submissions (and
  optionally a worker), waits for completion, and reports: submitted, completed, lost, duplicated
  executions, failover time. Writes `results/failover-drill.csv`.

### Tests

- `OrderedApplyTest`: out-of-order replicate calls are applied strictly by `seq_no`.
- `PromotionIT`: 3 schedulers + 3 workers in-process; 100 tasks; primary stopped at task 50 →
  100 COMPLETED, 0 lost, 0 duplicate executions (checked from worker execution logs).
- `WorkerDeathIT`: worker killed while running long SLEEP tasks → they complete on another worker.
- `RejoinIT`: a stopped backup restarts, catches up via `SyncFrom`, and holds identical state.

### Acceptance checks

```bash
mvn -q verify
scripts/start-cluster.sh --schedulers 3 --workers 3
java -jar predisched-benchmark/target/predisched-benchmark.jar failover-drill --tasks 100 --kill-primary-at 50 --kill-worker-at 70
```

Expected: `lost=0 duplicated=0`, and a new leader within ~5 s (spec §5).

### Docs

`docs/components/fault-tolerance.md` with a sequence diagram of promotion; primary-backup row in
`docs/LAB-COVERAGE.md` with the drill output.
