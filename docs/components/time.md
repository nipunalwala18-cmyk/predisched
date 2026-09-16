# Time: logical clocks, physical clock sync, event log (Prompt 04)

Every node gets two notions of time: Lamport clocks for causal ordering of events,
and synchronised physical clocks for trustworthy timestamps in metrics and the ML
dataset. Logical time answers "what happened before what"; physical time answers
"when did the cluster think it happened". The ML pipeline needs both: causal
order for correct labels, synchronised wall time for comparable features.

## Logical time (`predisched-common/clock`)

- `LamportClock`: `tick()` for a local event or send, `update(received)` =
  `max(local, received)+1`, `current()`. `AtomicLong` CAS loop; concurrent ticks
  never duplicate.
- `LamportClientInterceptor` ticks and attaches `lamport-time` metadata to every
  outgoing call; `LamportServerInterceptor` updates the clock from it. Installed
  on every channel (via `Channels`, which takes the node id and clock) and every
  server — no service code handles clock rules by hand. The `lamport_time` fields
  already in the protos are stamped from the same clock at the four shared choke
  points (`SchedulerClient`, `Dispatcher`, `WorkerServiceImpl`,
  `SchedulerServiceImpl` responses).
- Both interceptors set MDC `node`/`lamport` (save/restore, never destructive —
  an early version cleared the caller's context and wiped the daemon thread's
  node id). Background threads set their own node MDC (daemon, dispatcher,
  worker pool, registration, heartbeats); pool threads also stamp the current
  tick. Result: every log line carries `[node] [L=lamport]`.
- `EventLog`: in-memory ring (5,000) of `Event(nodeId, lamport, wallTimeMs,
  type, taskId, details)` for SUBMIT, ENQUEUE, DISPATCH, START, COMPLETE, FAIL,
  CANCEL, REGISTER (+ ELECTION/COORDINATOR/FAILOVER from Prompts 05/07). Services
  emit via `NodeContext.emit`, which ticks for the local event. Persisted in
  Prompt 08; worker-local START/COMPLETE stay queryable per node until then.

A task's causal chain, merged and Lamport-ordered (`predisched events --task`):

```
217 | scheduler-1 | SUBMIT   | … | type=MATRIX_TASK priority=5
218 | scheduler-1 | ENQUEUE  | … | queueSize=0
219 | scheduler-1 | DISPATCH | … | worker=worker-2
224 | scheduler-1 | COMPLETE | … | worker=worker-2 execMs=12
```

(Worker-side START/COMPLETE ticks 220–223 live in worker-2's log:
`[worker-2] [L=223] … done in 12 ms on worker-worker-2-exec-1`.)

## Physical time

- `NodeClock`: `now() = System.currentTimeMillis() + simulatedOffsetMs + drift +
  correction`. Offsets/drift come from config for one-machine demos. Corrections
  slew at 200 ms/s and `now()` never steps backwards (monotonic clamp). **All
  product code reads wall time through `NodeClock`.**
- `proto/clock.proto` exactly as spec §8.6; every scheduler and worker serves
  `ClockService`.
- `BerkeleyTimeDaemon` (on scheduler-1 until Prompt 05 elects the coordinator):
  every `syncIntervalMs` (5 s) it polls `timePeers` with `GetTime`, compensates
  RTT/2, drops readings more than `outlierMs` (1 s) from the median, averages the
  rest including itself, and sends each node its `Adjust(offset)` — outliers
  excluded from the average but still corrected; unreachable nodes skip the round.
- `CristianSync` on workers when `settings.clockSync: cristian`: request server
  time, set `serverTime + RTT/2` (default is `berkeley`, daemon-driven).
- Every round keeps `ClockSyncRecord(nodeId, ts, offsetBeforeMs, offsetAfterMs,
  algorithm)`; `admin clocks` prints the latest round.

Measured convergence (2026-09-16, configured offsets −300/+120/+500 ms):

| node | configured | first round seen | after rounds |
| --- | --- | --- | --- |
| scheduler-1 | 0 | 0 | 0 |
| worker-1 | −300 | −2 | 0 |
| worker-2 | +120 | −43 | −1 |
| worker-3 | +500 | +616 | +2 |

First-round sightings already include slew-in-progress (rounds run every 5 s from
scheduler start, before all workers are up) plus bring-up jitter; the steady
state is ±2 ms. Daemon log: `berkeley round: avg offset applied, 4 nodes,
0 excluded` every 5 s.

## Design choices

- Clock sync works against a `PeerClock`/`Server` interface, not gRPC directly —
  `BerkeleyTest`/`CristianTest` run deterministic fakes (outlier excluded yet
  corrected; 40 ms RTT → within RTT/2 of server time).
- `InterceptorIT` asserts the chain over real channels: one task leaves
  client=1 < scheduler < worker with the scheduler's received value (1) strictly
  below the worker's received value.
- One transient `DEADLINE_EXCEEDED` heartbeat was observed during cluster
  bring-up (1 s scheduler thread starved ~2 s by startup load); it retried next
  second with no task impact — heartbeats are best-effort, liveness is decided by
  the 5 s `workerTimeoutMs`, not by any single beat.
