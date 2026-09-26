# Replication and consistency (lab Exp 5)

Scheduler task state can be replicated across the scheduler nodes through a `ReplicatedTaskStore`
with a selectable consistency model: **strong** (quorum, N = 3, W = 2, R = 2) or **eventual** (local
write, asynchronous replication, last-writer-wins by Lamport time). The rest of the scheduler only
sees the `TaskStore` interface, so nothing else changes when replication is switched on.

Built by `spec/prompts/07-replication-consistency.md`. Spec sections: §4 FR10, §5 (Consistency),
§10.5, §11 Exp 5, §17 (Replication row).

## Pieces (`predisched-replication`)

| Class | Role |
| --- | --- |
| `VersionedRecord` | One copy of a task: encoded record, per-task version, Lamport time, origin node. Holds the LWW rule. |
| `TaskCodec` | `TaskRecord` ↔ bytes via `TaskRecordProto` (new in `task.proto`); lossless, attempts included. |
| `LocalReplica` | This node's copies (`ConcurrentHashMap`) and its `ReplicationLog`; applies a copy only if newer. |
| `ReplicationLog` | Append-only, sequence-numbered changes; `from(seq)` feeds `SyncFrom` catch-up. |
| `ReplicaSet` | Peers from the election's `ClusterView` (same channels), per-peer injected delay and `isolate(peer)`. |
| `ConsistencyMode` | Strategy: `StrongConsistency` or `EventualConsistency`, chosen by config. |
| `ReplicatedTaskStore` | `TaskStore` over a mode; striped per-task locks around read-modify-write; `syncFrom(peer, seq)`. |
| `SessionToken` | Client-centric bonus: last version seen per task, for read-your-writes and monotonic reads. |
| `ReplicationServiceImpl` | `Replicate`, `Read` (`local_only` or the mode's read), `SyncFrom` (server stream). |

`proto/replication.proto` follows spec §10.5 and adds `origin_node` (LWW tie-break), `local_only`
on `ReadRequest`, and `lamport_time`, `origin_node` and `found` on `ReadResponse`.

## Strong: quorum arithmetic

N = 3 replicas, a write needs W = 2 acknowledgements, a read consults R = 2 replicas. Because
W + R = 4 > N = 3, every set of 2 replicas that acknowledged a write shares at least one replica
with every set of 2 a read consults, so a read always meets the latest successful write.

- **Write:** sent to both peers in parallel (after any injected delay); as soon as W − 1 = 1 peer
  acknowledges, this node applies it as the W-th copy and the write succeeds. If no peer acknowledges
  within `writeTimeoutMs`, it throws `ReplicationException` and this node never applied it, so a
  failed write leaves nothing readable. (With W = 2 of 3, failure means no peer acked either.)
- **Read:** this replica plus the first R − 1 = 1 peer to answer; the newest copy wins, and any of
  them holding an older copy is sent the newest one (read repair).
- Fewer than R replicas reachable: the read fails rather than risk a stale answer.

## Eventual: last-writer-wins

A write is applied locally and acknowledged at once, then sent to each peer after that peer's
injected delay by a per-peer single-threaded sender, so each peer receives changes in order. Reads
are local. Of two copies, the one with the **higher Lamport time wins; equal times are broken by
origin node id**. Receiving a copy merges its Lamport time into the local clock, so a write made
after seeing another is always newer, and every replica applying the rule keeps the same copy.
A change that cannot be delivered is dropped and counted; a replica that was down catches up with
`SyncFrom`, which streams every logged change from a sequence number.

## In the scheduler

`replication.enabled: true` (see `configs/replication.yaml`) makes `SchedulerNode` build a
`ReplicatedTaskStore` over the election peers and serve `ReplicationService` on the scheduler's
port; otherwise the scheduler keeps `InMemoryTaskStore`. `--replication-mode strong|eventual`
overrides the mode. A submit whose store write cannot reach its quorum is answered
`accepted=false` with the reason. `replication.delayMs` injects lag per peer for the demo.

## How to run and demo

```bash
mvn -q install -DskipTests
java -jar predisched-benchmark/target/predisched-benchmark.jar consistency-compare
# live: 3 schedulers, 1 worker; replication to node 1 is delayed 5 s (configs/replication.yaml)
scripts/start-cluster.ps1 -Config configs/replication.yaml -Schedulers 3 -Workers 1
predisched --config configs/replication.yaml --port 51053 submit --type SLEEP_TASK --input ms=300
predisched --config configs/replication.yaml replica read --node 1 <task-id>          # stale
predisched --config configs/replication.yaml replica read --node 1 <task-id>          # 5 s later
scripts/stop-node.ps1 all
scripts/start-cluster.ps1 -Config configs/replication.yaml -Schedulers 3 -Workers 1 -ReplicationMode strong
predisched --config configs/replication.yaml replica read --node 1 --local <task-id>  # own copy
predisched --config configs/replication.yaml replica read --node 1 <task-id>          # quorum read
```

(`predisched` is `java -jar predisched-client/target/predisched-client.jar`.) Each CLI call starts a
JVM, about 2 s, which is why the demo lag is 5 s: a 2 s lag would already be over by the first read.

## Real output

**1. Eventual mode, stale read then convergence** (leader 3 takes the write, node 1 lags 5 s):

```
--- eventual: submit to leader 3 at 23:12:18.545
accepted=true task_id=task-3e15dcce trace=15fe1841 lamport=164 message='queued'
[23:12:20.805] node 2 read: task-3e15dcce status=COMPLETED worker=worker-1 version=3 lamport=182 origin=scheduler-3
[23:12:23.014] node 1 read: task-3e15dcce not found
--- after the 5 s lag:
[23:12:30.384] node 3 read: task-3e15dcce status=COMPLETED worker=worker-1 version=3 lamport=182 origin=scheduler-3
[23:12:32.435] node 2 read: task-3e15dcce status=COMPLETED worker=worker-1 version=3 lamport=182 origin=scheduler-3
[23:12:34.480] node 1 read: task-3e15dcce status=COMPLETED worker=worker-1 version=3 lamport=182 origin=scheduler-3
```

**2. Strong mode, same lag:** node 1's own copy is missing, yet a read through node 1 is current,
because its quorum includes a replica that took the write.

```
--- strong: submit to leader 3 at 23:13:01.124
accepted=true task_id=task-5649fe91 trace=3e7fcd54 lamport=171 message='queued'
[23:13:03.762] node 1 own copy: task-5649fe91 not found
[23:13:06.194] node 1 read: task-5649fe91 status=COMPLETED worker=worker-1 version=3 lamport=228 origin=scheduler-3
```

**3. Strong mode with 2 of 3 replicas down** (schedulers 1 and 2 stopped):

```
accepted=false task_id=task-3871a288 message='could not store the task: write of task-3871a288 v1
got 0 of the 1 peer acks W=2 needs within 1000 ms; not applied'
```

## Measured: latency against consistency

`consistency-compare`: three in-process replicas, node 1 writes 500 new tasks per row, replication
to replica 2 delayed 5 ms and to replica 3 delayed 50 ms. Right after each write the task is read
through replica 3; a read that misses the write is stale. Convergence is the time from the last
write until every live replica holds identical state. `results/exp5-consistency.csv`:

| Mode | Replicas down | Failed writes | Write p50 ms | Write p95 ms | Stale reads / 500 | Convergence ms |
| --- | --- | --- | --- | --- | --- | --- |
| strong | 0 | 0 | 6.31 | 7.40 | 0 | 3 |
| strong | 1 (replica 2) | 0 | 57.08 | 58.47 | 0 | 0 |
| eventual | 0 | 0 | 0.01 | 0.02 | 500 | 94 |
| eventual | 1 (replica 2) | 0 | 0.00 | 0.01 | 500 | 112 |

- Strong never returned a stale read. It pays for that in write latency: its writes wait for the
  fastest peer, about 6 ms here, and with replica 2 down they wait for the 50 ms-lagged replica 3,
  a 9x jump. With two replicas down it stops accepting writes (demo 3).
- Eventual acknowledged every write in microseconds, whatever was down, and every immediate read
  of the lagging replica was stale; replicas converged about 100 ms after the last write.
- Strong converges almost at once because its quorum reads repaired replica 3 along the way.

## Tests

`StrongConsistencyTest`: 150 random writes and updates from random writers with a lagging replica,
each followed by a read through every replica, all current; 2 of 3 down, the write fails and no
replica holds any trace of it; a quorum read repairs the stale replica it consulted.
`EventualConsistencyTest`: with a 2 s delay the lagging replica reads the old value and all three
agree within 3 s; concurrent updates from two nodes converge to the winning origin's value on all
replicas; `SyncFrom` brings a fresh replica to the leader's sequence number; a `SessionToken` read on
a lagging replica sees its own write. `TaskCodecTest`: every field survives the round trip.

Not done yet: an admin command to isolate a peer at runtime (the switch exists in `ReplicaSet`;
TODO(prompt 22) exposes it through the admin API), and automatic catch-up of a replica that was
down (TODO(prompt 10) runs `SyncFrom` when a backup rejoins).
