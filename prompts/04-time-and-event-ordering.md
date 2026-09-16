# Prompt 04 — Time: logical clocks, physical clock sync, ordered event log

**Spec sections:** §4 (FR8), §5 (Clock accuracy, Observability), §8.6, §9 "Exp 3"
**Depends on:** 03
**Lab topics:** Clock synchronization (logical and physical)
**Commit message:** `Time: Lamport clocks on every message, Berkeley and Cristian clock sync, event log`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Give every node a notion of time the rest of the
product can trust: Lamport clocks for causal ordering of events, and synchronised physical clocks for
timestamps in metrics and the ML dataset.

### Logical time (`predisched-common`, package `clock`)

- `LamportClock`: `tick()` for a local event or send, `update(long received)` = `max(local, received)+1`,
  `current()`. Thread-safe with `AtomicLong` and a CAS loop.
- A gRPC `ClientInterceptor` that ticks and attaches `lamport-time` metadata to every outgoing call,
  and a `ServerInterceptor` that updates the clock from incoming metadata. Install both on every
  channel and server in the product — no service code handles clocks by hand. Also fill the
  `lamport_time` fields already in the protos from the same clock.
- The interceptors set MDC `node` and `lamport` so every log line carries them.
- `EventLog`: records `Event(nodeId, lamport, wallTimeMs, type, taskId, details)` for SUBMIT, ENQUEUE,
  DISPATCH, START, COMPLETE, FAIL, CANCEL, REGISTER, and (later) ELECTION, COORDINATOR, FAILOVER.
  In-memory ring now; persisted in Prompt 08.

### Physical time

- `NodeClock` in `common`: `now()` = `System.currentTimeMillis() + simulatedOffsetMs + drift + correction`.
  `simulatedOffsetMs` and `driftPpm` come from config, so skew can be demonstrated on one machine.
  **All product code reads wall time through `NodeClock`**, never `System.currentTimeMillis()` directly.
- `proto/clock.proto` exactly as §8.6. Every scheduler and worker serves `ClockService`.
- `BerkeleyTimeDaemon` (runs on the scheduler that coordinates the cluster — a fixed scheduler until
  Prompt 05 makes it the elected leader): every `syncIntervalMs`, polls all nodes with `GetTime`,
  compensates each reading with RTT/2, discards readings more than `outlierMs` from the median,
  averages including itself, and sends each node its own `Adjust(offset)`. Correction is slewed
  gradually, never stepping time backwards.
- `CristianSync` on workers as the configurable alternative (`settings.clockSync: berkeley|cristian`):
  request server time, set `serverTime + RTT/2`.
- Every round writes a `ClockSyncRecord(nodeId, ts, offsetBeforeMs, offsetAfterMs, algorithm)`.
- CLI: `predisched admin clocks` prints each node's current offset from the daemon; and
  `predisched events [--task <id>]` prints the cluster's events merged and sorted by
  (Lamport time, node id).

### Tests

- `LamportClockTest`: tick, update rules, concurrent ticks never produce duplicates.
- `InterceptorIT`: client → scheduler → worker → scheduler; the Lamport time strictly increases along
  the causal chain for one task.
- `BerkeleyTest` with fake node clocks at −300, +120 and +500 ms: after one round all offsets are
  within 5 ms of the mean; an outlier at +5,000 ms is excluded from the average but still corrected.
- `CristianTest`: with a fake 40 ms RTT the result is within RTT/2 of server time.

### Acceptance checks

```bash
mvn -q verify
scripts/start-cluster.sh    # worker configs given offsets -300, +120, +500 ms
java -jar predisched-client/target/predisched-client.jar admin clocks    # before and after a round
java -jar predisched-client/target/predisched-client.jar events --task <id>
```

### Docs

`docs/components/time.md` (why logical and physical time both exist in this product);
clock-synchronization row in `docs/LAB-COVERAGE.md` with the before/after offset table.
