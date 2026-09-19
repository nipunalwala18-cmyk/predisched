# Prompt 03: Lamport clocks, Berkeley and Cristian sync, trace ids (Exp 3, F10)

## Goal

Every message and log line carries a Lamport timestamp and the node id, and each task carries a trace
id from client to worker. The scheduler runs Berkeley rounds that shrink artificial physical clock
offsets, and workers can use Cristian's algorithm instead. A Lamport-ordered event log exists. This
is lab Exp 3 plus feature F10.

## Read first

- Spec §4 FR8 and FR38, §5 (Clock accuracy, Observability), §10.6, §11 Exp 3, §15.8, F10 in §6

## Build

1. **Protos**: `proto/clock.proto` with `TimeRequest`, `TimeResponse`, `ClockAdjust`,
   `ClockService` (`GetTime`, `Adjust`), as in spec §10.6. Add `string trace_id` to `TaskRequest`
   with the next free field number.
2. **`predisched-common`**
   - `LamportClock`: `tick()`, `update(received)` = `max(local, received) + 1`, `current()`.
     Thread-safe with `AtomicLong` and a CAS loop.
   - `PhysicalClock`: system time plus an artificial offset and drift rate from config (spec: e.g.
     −300 ms to +500 ms). `adjust(offsetMs)` applies a correction. All code that needs "now" for
     ordering or logging uses this, not `System.currentTimeMillis()`.
   - gRPC interceptors (`ClientInterceptor` + `ServerInterceptor`) that send the Lamport time and
     trace id as metadata on every call and update the clock on receipt, so no service code has to
     remember to do it. The `lamport_time` proto fields are also filled for visibility.
   - Logging: Logback config with MDC keys `node`, `lamport`, `trace`, in a pattern
     `%d %-5level [%X{node} L=%X{lamport} %X{trace}] %logger{20} - %msg%n`, plus a JSON appender
     writing `logs/<node>.jsonl` for the Logs page later.
   - `EventLog`: appends structured events (node, Lamport time, physical time, type, details) to the
     JSON log. Emit events for submit, enqueue, dispatch, execute start/end, result.
3. **Clock sync**
   - `ClockServiceImpl` on every node (scheduler and worker).
   - `BerkeleyDaemon` in the scheduler: every `clock.sync.interval.ms`, poll all known nodes with
     `GetTime`, correct each reading by RTT/2, drop readings more than `clock.outlier.ms` from the
     median, average, send each node its own `Adjust`. Log offsets before and after.
   - `CristianClient` in the worker: selectable by config instead of Berkeley for that worker.
4. **Trace ids**: the client generates a UUID per task (or accepts `--trace`), the scheduler keeps it
   on the record, and the worker logs it. `status` shows it.
5. **Tooling**
   - `scripts/merge-events.py`: merge all `logs/*.jsonl` and print two columns side by side:
     events ordered by physical time and by (Lamport, node id). This is the Exp 3 demo.

## Tests

- `LamportClockTest`: tick and update rules; concurrent ticks never repeat a value.
- `BerkeleyDaemonTest` with fake nodes at offsets −300, 0, +200, +500 ms: after one round the spread
  is under 10 ms; an outlier node is excluded from the average but still corrected.
- `CristianClientTest` with a fake server and injected RTT.
- Interceptor test: a call from node A (Lamport 10) to node B (Lamport 3) leaves B at 11.

## Acceptance checks

Start a scheduler and two workers with offsets `-300` and `+500`, then:

```bash
java -jar predisched-client/target/predisched-client.jar submit --type CPU_TASK --input n=1000000 --priority 5
```
```bash
python scripts/merge-events.py logs
```

Paste the Berkeley before/after offsets from the scheduler log and the merged event table.

## Docs and commit

- `docs/components/clocks.md`: both algorithms, interceptor design, measured offsets before/after.
- `docs/LAB-COVERAGE.md` Exp 3 row.
- Commit: `Time: Lamport clocks on every message, Berkeley and Cristian clock sync, event log`
