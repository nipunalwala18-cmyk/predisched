# Prompt 07: Replicated task state with strong and eventual consistency (Exp 5)

## Goal

Scheduler task state is replicated across nodes through a `ReplicatedTaskStore` with a selectable
consistency model: strong (quorum N=3, W=2, R=2) or eventual (async with last-writer-wins by Lamport
time). A demo shows a stale read in eventual mode, convergence afterwards, and no stale read in
strong mode, with a measured latency vs consistency table. This is lab Exp 5.

## Read first

- Spec §4 FR10, §5 (Consistency), §10.5, §11 Exp 5, §17 (Replication row)

## Build

1. **Protos**: `proto/replication.proto` with `ReplicateRequest`, `ReadRequest`, `ReadResponse`,
   `SyncRequest` and `ReplicationService` (`Replicate`, `Read`, `SyncFrom` streaming) as in spec
   §10.5. Add a `string origin_node` field for last-writer-wins tie-breaks.
2. **`predisched-replication`**
   - `VersionedRecord`: payload bytes, version, Lamport time, origin node.
   - `TaskCodec`: `TaskRecord` ↔ bytes via a `TaskRecordProto` message (add it to `task.proto`).
   - `ReplicationLog`: append-only, sequence-numbered list of operations, thread-safe, with
     `from(seq)` for catch-up.
   - `ReplicaSet`: peer channels from `ClusterView`, with per-peer injected delay and an
     `isolate(peer)` switch for demos (config and admin CLI).
   - `ReplicatedTaskStore implements TaskStore` with two modes behind a `ConsistencyMode` strategy
     (no `if` chains in callers):
     - **Strong**: write locally + to peers in parallel, succeed when W acks arrive (including self)
       within `replication.write.timeout.ms`, else throw `ReplicationException`. Reads ask R replicas
       and return the highest version, repairing stale replicas (read repair).
     - **Eventual**: write locally, ack immediately, replicate asynchronously on a single-threaded
       executor per peer (preserves order) after the injected delay. Reads are local. Conflicts:
       higher Lamport wins, ties broken by origin node id.
   - Client-centric (bonus, keep small): `SessionToken` holding the last version seen per task; a
     read with a token that finds an older local version fetches from a peer (read-your-writes and
     monotonic reads).
   - `ReplicationServiceImpl` serving `Replicate`, `Read`, `SyncFrom`.
3. **`predisched-scheduler`**: `SchedulerNode` uses `ReplicatedTaskStore` when
   `replication.enabled: true`, else `InMemoryTaskStore`. The scheduler depends only on the
   `TaskStore` interface.
4. **`predisched-benchmark`**: `ConsistencyCompare` command. On an in-process 3-node cluster, for each
   mode: 500 writes then reads, with 0 and then 1 replica down. Records write p50/p95 latency,
   failed writes, stale reads observed, and time to convergence. Writes
   `results/exp5-consistency.csv`.
5. **CLI**: `predisched replica read --node <id> <taskId>` reads from one specific replica, for the
   stale-read demo.

## Tests

- Strong mode: after any successful write, a read from any 2 of 3 replicas returns it (property test
  over random interleavings with one lagging replica).
- Strong mode with 2 of 3 replicas down: writes fail, nothing half-applied is readable.
- Eventual mode: with a 2 s injected delay, an immediate read on the lagging replica is stale, and
  all replicas are equal within delay + 1 s.
- LWW: concurrent writes to one task from two nodes converge to the same value on all replicas.
- `SyncFrom` brings a fresh replica to the leader's sequence number.

## Acceptance checks

```bash
java -jar predisched-benchmark/target/predisched-benchmark.jar consistency-compare
```

Also run the three-step demo from spec §11 Exp 5 on a live 3-node cluster with the CLI and paste the
stale read, the converged read, and the strong-mode read.

## Docs and commit

- `docs/components/replication.md`: both modes, quorum arithmetic, LWW rule, the measured table.
- `docs/LAB-COVERAGE.md` Exp 5 row.
- Commit: `Replication: task state replicated with quorum or eventual consistency`
