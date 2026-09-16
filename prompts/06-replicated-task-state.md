# Prompt 06 — Replicated task state with selectable consistency

**Spec sections:** §4 (FR10), §5 (Consistency), §8.5, §9 "Exp 5"
**Depends on:** 05
**Lab topics:** Data consistency and replication models
**Commit message:** `Replication: task store replicated across schedulers with quorum or eventual consistency`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Replace the scheduler's in-memory `TaskStore` with a
replicated one, so task state survives on more than one node. Consistency is a product setting with
real trade-offs, and the product must be able to show them.

### Build (`predisched-replication`)

- `proto/replication.proto` exactly as §8.5.
- `VersionedRecord`: payload (serialized `TaskRecord`), `version`, `lamportTime`, `writerNodeId`.
- `ReplicatedTaskStore implements TaskStore` (the interface from Prompt 01), with a local
  `ConcurrentHashMap` and a `ReplicaSet` of peer stubs. Mode from `settings.consistency`:
  - **`strong`** — quorum: N = replica count, W and R from config with a startup check that
    W + R > N (default N=3, W=2, R=2). A write is applied locally, sent to all replicas in parallel,
    and succeeds once W acknowledgements (including self) arrive within `writeTimeoutMs`; otherwise
    it fails and the caller sees the failure. A read asks R replicas and returns the highest version,
    then read-repairs any stale replica asynchronously.
  - **`eventual`** — apply locally, acknowledge immediately, replicate asynchronously through a
    per-peer queue with retry. An optional `replicationDelayMs` injects lag so staleness is visible.
    Conflicts resolve by last-writer-wins on (lamportTime, writerNodeId).
- Every applied write appends to a `ReplicationLog` with a monotonically increasing `seq_no`; `SyncFrom`
  streams entries from a sequence number (used for catch-up in Prompt 07).
- Anti-entropy for eventual mode: every `antiEntropyMs`, replicas exchange a digest (task id →
  version) and pull what they are missing, so they converge even after dropped messages.
- **Client-centric guarantees:** `SchedulerClient` remembers the highest version it has seen per task
  and sends it as a minimum version on reads; the serving node returns the value only if it is at
  least that version, otherwise reads from a peer — giving read-your-writes and monotonic reads.

### Integrating it with the product

- `SchedulerNode` uses `ReplicatedTaskStore` when more than one scheduler is configured, the in-memory
  store otherwise. No other scheduler code changes — if it has to, the `TaskStore` interface is wrong;
  fix the interface.
- `predisched get <id> --from <schedulerId>` reads from one specific replica (a debugging aid that also
  demonstrates staleness).

### Tests

- `QuorumTest`: with one of three replicas down, writes and reads still succeed; with two down, writes
  fail; a read after a successful write never returns an older version (property test, 1,000 random
  interleavings).
- `EventualTest`: with 500 ms injected delay, a read from a lagging replica returns the old version,
  and all replicas converge within delay + anti-entropy interval.
- `LwwTest`: concurrent conflicting writes resolve identically on every replica.
- `ClientGuaranteesTest`: a client never observes its version go backwards.

### Acceptance checks

```bash
mvn -q verify
java -jar predisched-benchmark/target/predisched-benchmark.jar consistency-compare
```

`consistency-compare` runs a 1,000-write workload in both modes with all replicas up and with one
down, and records write latency (mean, P95), read staleness rate, availability (% writes accepted) and
time to convergence, into `results/consistency-compare.csv`.

### Docs

`docs/components/replication.md`; replication row in `docs/LAB-COVERAGE.md` with the table and a
stale-read-then-converge log excerpt.
