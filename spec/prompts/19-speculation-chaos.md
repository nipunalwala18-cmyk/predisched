# Prompt 19: Speculative execution for stragglers and a chaos API (F11, F16)

## Goal

A straggling task gets a speculative duplicate on a faster worker; the first result wins and the loser
is cancelled. A chaos API can kill a worker or the primary, inject latency, spike CPU, partition a
replica, drain a worker and fire a burst, so failures can be shown on demand.

## Read first

- Spec §4 FR28, FR35, §6 Tier 2 (F11, F16), §15.7, §7.3 (`long_tail`)

## Build

1. **Speculative execution (F11)** in `predisched-scheduler`:
   - `StragglerDetector` runs every `speculation.check.ms`: a running task is a straggler when its
     elapsed time exceeds `max(k × predicted_exec, p90 exec for its type)` (k from config, default 2;
     uses the type p90 alone when no prediction exists).
   - The duplicate is placed by the active strategy with the original worker excluded, only if a
     candidate has spare capacity, and at most one duplicate per task.
   - First result wins through a compare-and-set on the task record; the other attempt is cancelled
     with `CancelExecution` and recorded as `SPECULATIVE_LOSER`. Idempotent completion from prompt 10
     makes a late result harmless.
   - Counters: speculations launched, won by the duplicate, wasted work in ms.
2. **Chaos API**: `proto/chaos.proto` with `ChaosService` served by every node:
   `InjectLatency(ms, duration)`, `SpikeCpu(threads, duration)`, `Isolate(duration)` (drops all
   replication traffic), `Drain()` (worker: finish current work, accept nothing new), `Crash()` (exit
   the process with a log line). Latency injection is a server interceptor. Enabled only when
   `chaos.enabled: true`.
3. **Chaos CLI**: `predisched chaos kill-worker <id> | kill-primary | latency <node> <ms> <sec> |
   cpu <worker> <sec> | partition <node> <sec> | drain <worker> | burst <n> --profile bursty`. Each
   action writes a `CHAOS` event (with Lamport time) so charts can mark it later.
4. **Dispatch honours draining**: drained workers leave the candidate list; they report `DRAINING`
   in heartbeats.

## Tests

- Detector threshold with and without a prediction.
- A `long_tail` trace with one injected slow worker: at least one speculative duplicate wins, the
  loser is cancelled, and each task completes exactly once.
- Each chaos action on an in-process node has its effect and ends after its duration.
- Draining: no new dispatch to a drained worker; its running tasks finish.

## Acceptance checks

```bash
java -jar predisched-client/target/predisched-client.jar chaos latency worker-2 1500 60
```
```bash
java -jar predisched-client/target/predisched-client.jar workload replay <any long_tail trace in workloads/campaign/>
```
```bash
java -jar predisched-client/target/predisched-client.jar chaos kill-primary
```

Paste the speculation counters from the replay, one straggler's attempt history, and the election
and promotion lines after `kill-primary`.

## Docs and commit

- `docs/components/speculation-and-chaos.md`.
- Commit: `Resilience: speculative execution for stragglers and a chaos fault-injection API`
