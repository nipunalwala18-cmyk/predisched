# Prompt 06: Leader election with Bully and Ring (Exp 4)

## Goal

Five scheduler nodes (ids 1–5) elect a coordinator with Bully (default) or Ring, chosen by config.
Killing the leader triggers a new election within about 5 s, and exactly one leader emerges. Message
count and election time are measured for both algorithms. This is lab Exp 4.

## Read first

- Spec §4 FR9, §5 (Availability), §10.3, §11 Exp 4, §17 (Election row)

## Build

1. **Protos**: `proto/election.proto` with `ElectionMsg`, `CoordinatorMsg` and `ElectionService`
   (`Election`, `RingPass`, `Coordinator`, `Ping`) as in spec §10.3. Reuse `Ack` from `common.proto`.
2. **`predisched-election`**
   - `ElectionAlgorithm` interface: `start()`, `onElection(msg)`, `onRingPass(msg)`,
     `onCoordinator(msg)`, `leader()` (an `Optional<Integer>`), and a listener for leader changes.
   - `ClusterView`: node id → address for all peers, from config.
   - `BullyElection`: on leader timeout send `Election` to all higher ids; any `OK` (a successful
     `Ack`) means wait for `Coordinator` with its own timeout; none within `election.timeout.ms`
     means declare self leader and broadcast `Coordinator`. A lower-id `Election` received is answered
     with `OK` and triggers our own election.
   - `RingElection`: the initiator sends `RingPass` with its id to its successor; each node appends
     its id and forwards, skipping unreachable successors; when the message returns to the initiator,
     the max id is the leader and a `Coordinator` message circulates once.
   - `LeaderMonitor`: non-leaders `Ping` the leader every `election.ping.interval.ms`; after
     `election.ping.misses` failures they start an election.
   - `ElectionStats`: `AtomicLong` counters for messages sent per type and the duration from
     detection to `Coordinator` received. Exposed in logs and to later prompts.
   - Every election event goes to the `EventLog` with Lamport time (prompt 03).
3. **`predisched-scheduler`**
   - `SchedulerNode` wires the election module in. Only the leader accepts `SubmitTask` and dispatches;
     followers answer `SubmitTask` with `accepted=false` and a `leader=<id>` hint in the message.
     Full state hand-over is prompt 10; for now a new leader starts with an empty queue.
   - The Berkeley daemon from prompt 03 runs only on the leader.
4. **Scripts**: `scripts/start-cluster.ps1` and `scripts/start-cluster.sh` start 5 schedulers
   (ports 50051–50055) and 3 workers (50061–50063) with logs in `logs/`, and a matching
   `scripts/stop-node` that kills one node by id.
5. **CLI**: `predisched cluster leader` asks every scheduler who it thinks the leader is.

## Tests

- `BullyElectionTest` over in-process gRPC with 5 nodes: initial election picks 5; kill 5 → 4; kill
  4 and 5 → 3; nodes 3 and 4 detecting the failure at the same time still yield exactly one leader.
- `RingElectionTest`: the same scenarios, plus a dead node in the middle of the ring is skipped.
- Both report message counts; assert Bully is O(n²) worst case and Ring O(n) per round in the counts.

## Acceptance checks

```bash
scripts/start-cluster.sh
```
```bash
java -jar predisched-client/target/predisched-client.jar cluster leader
```
```bash
scripts/stop-node.sh scheduler 5
```

Run `cluster leader` again after about 5 s. Repeat with `election.algorithm: ring`. Paste both
leader outputs, the election log lines, and a table of message count and election time for each
algorithm (from `ElectionStats`, 5 runs each) into `results/exp4-election.csv`.

## Docs and commit

- `docs/components/election.md`: both algorithms with a sequence diagram each, timeouts, the
  measured comparison table.
- `docs/LAB-COVERAGE.md` Exp 4 row.
- Commit: `Leadership: scheduler cluster elects a coordinator with Bully or Ring`
