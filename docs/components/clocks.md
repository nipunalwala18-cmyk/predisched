# Clocks and tracing (lab Exp 3: clock synchronisation, plus F10)

Every message PrediSched sends carries a Lamport timestamp and, when one is in scope, a trace id.
Every log line shows both alongside the node id. Physical clocks are deliberately skewed so
synchronisation has something to correct, and either Berkeley or Cristian's algorithm brings them
back together.

Built by `spec/prompts/03-clocks-and-tracing.md`. Spec sections: §4 (FR8, FR38), §5 (Clock accuracy,
Observability), §10.6, §11 Exp 3, §15.8.

## Logical time: Lamport clocks

`LamportClock` (in `predisched-common`) implements the two rules with an `AtomicLong` and a CAS
loop, so concurrent ticks never hand out the same value:

```
tick()            local event or send:  L = L + 1
update(received)  on receipt:           L = max(L, received) + 1
```

Three kinds of event advance it:

1. **Sending a request.** The client interceptor ticks and writes `x-lamport-time` into the headers.
2. **Receiving anything.** The server interceptor merges the sender's value.
3. **Recording a local event.** `EventLog.event()` ticks, so two events on one node never share a
   timestamp and the ordering demo can separate them.

**Replies count as messages too.** The server stamps its response headers and the client merges that
value. Without this the caller could log a result with a *lower* Lamport time than the work it was
reporting, which is exactly the causality violation the experiment is meant to rule out. This was
visible in the first demo run and is what the reply-side stamping fixed.

## The interceptors

`LamportInterceptors.client(clock)` and `.server(nodeId, clock)` are installed on every channel and
every server in the system. That is the whole reason no service method contains clock code: a new
RPC added in a later prompt gets Lamport time and trace propagation for free.

The server interceptor also fills the MDC (`node`, `lamport`, `trace`) for the duration of the call
and clears the trace afterwards, so an id cannot leak onto the next call that reuses the thread.
Background threads (Berkeley daemon, heartbeats, cluster reporter, dispatcher) call
`LamportInterceptors.applyMdc()` themselves, since no gRPC call started them.

## Physical time: skew, drift and two algorithms

`PhysicalClock` is the system clock plus an artificial `offsetMs` and a `driftPpm` rate, and
`adjust(correction)` moves it. Everything that needs "now" for ordering or timestamps reads it
through `Clocks.now()` rather than `System.currentTimeMillis()`.

### Berkeley (default)

`BerkeleyDaemon` runs on the scheduler, which acts as the time daemon. Each round:

1. poll every registered worker with `GetTime`;
2. compare each reply with **the coordinator's own clock read around that call**, taking the
   midpoint of send and receive, so the network delay is not charged as a clock offset;
3. drop readings more than `clock.outlierMs` from the median;
4. average what is left, then send every node (outliers included) its own `Adjust`;
5. move its own clock by the same rule. No node's clock is treated as correct.

The midpoint detail matters. The first version sampled the coordinator's time once at the start of
the round, so the connection setup on the first call (about 180 ms) looked like every worker being
180 ms ahead. The round then "corrected" the cluster into a real 180 ms skew, and the logs showed a
stable −170 ms error that never converged. Measuring per call fixed it.

### Cristian's algorithm (alternative)

`CristianClient` runs on a worker configured with `clock.algorithm: cristian`. It asks the scheduler
for the time and sets its clock to `server_time + RTT / 2`. It is one-sided: the server's time wins
and the server never moves.

The two are alternatives, not layers. `configs/cristian.yaml` switches Berkeley off for that reason;
running both would correct a worker twice per round.

## Trace ids (F10, FR38)

The client generates a short trace id per task (or takes `--trace`), the scheduler keeps it on the
`TaskRecord`, the dispatcher puts it back in scope when it sends the task on, and the worker's
execution threads adopt it. `status` and `watch` print it, so one id ties the client's submission to
the worker's log lines.

## Event log

`EventLog` writes one JSON object per line to `logs/<node>.jsonl` with node, Lamport time, physical
time, wall time, type, task id and trace. Events: `SUBMIT`, `ENQUEUE`, `DISPATCH`, `EXECUTE_START`,
`EXECUTE_END`, `RESULT`, `CLOCK_SYNC`.

A note on the prompt: it asked for a Logback JSON appender. The structured events are written
directly by `EventLog` instead, because typed fields (task id, worker, exec ms) are what the Logs
page and `merge-events.py` need, and a pattern-encoded appender would only produce JSON-shaped text.
Logback still writes the human-readable `logs/<node>.log` and the console.

## How to run and demo

A scheduler and two workers with opposite skew:

```bash
java -jar predisched-scheduler/target/predisched-scheduler.jar --id scheduler-1 --port 51051 --config configs/local.yaml
```
```bash
java -jar predisched-worker/target/predisched-worker.jar --id worker-1 --port 51061 --clock-offset -300 --config configs/local.yaml
```
```bash
java -jar predisched-worker/target/predisched-worker.jar --id worker-2 --port 51062 --clock-offset 500 --config configs/local.yaml
```
```bash
python scripts/merge-events.py logs --limit 14
```

For Cristian instead: `--config configs/cristian.yaml`.

## Real output

Log lines now carry node and Lamport time (rule 6):

```
09:57:53.453 INFO [scheduler-1 L=22 ] c.p.s.RegistryServiceImpl - Registered worker worker-2 ...
```

### Berkeley: offsets before and after

Workers started at −300 ms and +500 ms; `syncIntervalMs` 10 s.

```
Berkeley round: offsets before {scheduler-1=0, worker-1=-294, worker-2=513} (spread 807 ms), average 73 ms, excluded []
Berkeley round: corrections {scheduler-1=73, worker-1=367, worker-2=-440}, offsets after {scheduler-1=73, worker-1=73, worker-2=73} (spread 0 ms)
Berkeley round: offsets before {scheduler-1=0, worker-1=-6, worker-2=-13} (spread 13 ms), average -6 ms
Berkeley round: offsets before {scheduler-1=0, worker-1=2, worker-2=1} (spread 2 ms), average 1 ms
```

The measured offsets (−294, +513) match the configured skew within measurement noise, and the spread
falls **807 ms → 13 ms → 2 ms** over three rounds. Residual movement of a few ms per round is the
round-trip estimate, not drift.

The workers log the corrections they were told to apply:

```
[worker-1 L=10 ] c.p.c.t.ClockServiceImpl - Clock adjusted by 367 ms
[worker-2 L=10 ] c.p.c.t.ClockServiceImpl - Clock adjusted by -440 ms
```

### Cristian: a worker 450 ms behind

```
Cristian sync with scheduler-1: rtt=36 ms, offset was 461 ms, corrected to 0 ms
Cristian sync with scheduler-1: rtt=40 ms, offset was 4 ms, corrected to 0 ms
Cristian sync with scheduler-1: rtt=10 ms, offset was -13 ms, corrected to 0 ms
```

### Physical order vs Lamport order

`python scripts/merge-events.py logs --limit 14`, two tasks and three clock-sync rounds:

```
PHYSICAL TIME ORDER (each node its own clock)            | LAMPORT ORDER
---------------------------------------------------------+------------------------------------------
 40908 scheduler-1  DISPATCH       7dee32a3              |     27 scheduler-1  DISPATCH       7dee32a3
 41423 worker-1     EXECUTE_START  7dee32a3 worker-1-exec-1 |     34 worker-1     EXECUTE_START  7dee32a3
 41471 worker-1     EXECUTE_END    7dee32a3              |     35 worker-1     EXECUTE_END    7dee32a3
 41797 scheduler-1  RESULT         7dee32a3 46ms         |     38 scheduler-1  RESULT         7dee32a3 46ms
 44189 scheduler-1  CLOCK_SYNC     - spread 829->0ms     |     53 scheduler-1  CLOCK_SYNC     - spread 829->0ms
 49606 worker-1     EXECUTE_START  cc66992a worker-1-exec-2 |     77 scheduler-1  SUBMIT         cc66992a
 49617 scheduler-1  SUBMIT         cc66992a              |     78 scheduler-1  ENQUEUE        cc66992a
 49618 scheduler-1  ENQUEUE        cc66992a              |     80 scheduler-1  DISPATCH       cc66992a
 49619 scheduler-1  DISPATCH       cc66992a              |     83 worker-1     EXECUTE_START  cc66992a
```

This is the experiment's point in one screen. By physical time, worker-1 *starts executing*
`cc66992a` eleven milliseconds **before** the scheduler records submitting it, which cannot have
happened: worker-1's clock still sits behind the scheduler's. The Lamport column puts the five
events in the only order that respects causality, because the worker's clock merged the dispatch
message's timestamp when it arrived.

## Tests

`mvn -q verify` runs 54 tests. New here:

- `LamportClockTest`: tick and update rules; 8 threads × 2,000 ticks hand out 16,000 distinct
  values; updates never move the clock backwards; offset, drift and corrections on `PhysicalClock`.
- `ClockSyncTest`: a Berkeley round over in-process gRPC brings −300, 0 and +200 ms clocks within
  10 ms; a node 500 s out is excluded from the average but still corrected onto the agreed time;
  Cristian applies `server_time + RTT / 2` with a scripted round trip.
- `LamportInterceptorsTest`: a call from node A at 10 leaves B at 12 and A at 14 once the reply is
  merged; the trace id is visible inside the call and gone afterwards; a call with no header still
  ticks.

## Known limits, resolved later

- The scheduler is the time daemon because it is the only scheduler; once there is an elected
  coordinator (prompt 06), that node takes the role.
- `clock_sync` rows go to the event log, not yet to PostgreSQL (prompt 11).
- The Logs page that reads `logs/*.jsonl` arrives with the dashboard (prompt 23).
