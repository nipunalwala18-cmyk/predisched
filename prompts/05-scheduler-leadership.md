# Prompt 05 — Scheduler cluster leadership (Bully and Ring)

**Spec sections:** §4 (FR9), §5 (Availability), §8.3, §9 "Exp 4"
**Depends on:** 04
**Lab topics:** Bully and Ring election
**Commit message:** `Leadership: scheduler cluster elects a coordinator with Bully or Ring`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Run schedulers as a cluster of 3–5 nodes with exactly
one coordinator. Only the coordinator accepts task submissions, dispatches, and runs the Berkeley
time daemon; the others stand by (they become real backups in Prompt 07).

### Build (`predisched-election`)

- `proto/election.proto` exactly as §8.3.
- `ElectionAlgorithm` interface: `start()`, `onElection(msg)`, `onRingPass(msg)`,
  `onCoordinator(msg)`, `currentLeader()`, and a listener `onLeaderChanged(int leaderId)`.
- `BullyElection`: on leader timeout send `Election` to all higher ids; any `OK` → wait
  `coordinatorTimeoutMs` for a `Coordinator`, restarting the election if none arrives; no `OK` within
  `electionTimeoutMs` → become leader and broadcast `Coordinator`. A node with a higher id that starts
  up triggers an election (it bullies its way in).
- `RingElection`: logical ring ordered by id; `RingPass` accumulates ids, skipping unreachable
  successors; when the message returns to the initiator the highest id wins and `Coordinator`
  circulates once round the ring. Concurrent initiators must still converge on one leader.
- `LeaderMonitor`: followers `Ping` the leader every `pingMs`; `missedPings` consecutive failures start
  an election. An election already in progress is not started twice.
- `ElectionStats`: messages sent and time from detection to agreed leader, per election.
- Choice via `settings.election: bully|ring`. Every election writes ELECTION / COORDINATOR events to
  the event log with Lamport times.

### Integrating it with the product

- `SchedulerNode` wires `onLeaderChanged` to a `Role` state (`LEADER` / `FOLLOWER`). A follower
  rejects `SubmitTask` with `accepted=false` and message `NOT_LEADER:<leaderHost:port>`.
- `SchedulerClient` (Prompt 01) takes a list of scheduler addresses, follows `NOT_LEADER` redirects and
  retries the next address on `UNAVAILABLE`. Workers register and heartbeat to whichever node is leader
  the same way.
- The Berkeley daemon starts and stops with leadership.
- `predisched admin leader` prints the leader as seen by each scheduler.

### Tests

- Unit tests for both algorithms over an in-memory message bus (no gRPC) with injectable node
  failures: highest alive id always wins; killing the leader elects the next highest; two nodes
  starting elections at once still yield one leader; a node recovering with the highest id takes over
  (Bully).
- `LeadershipIT`: 5 in-process schedulers over gRPC; stop node 5 → node 4 is leader on all survivors
  within 5 s; stop node 4 → node 3.

### Acceptance checks

```bash
mvn -q verify
scripts/start-cluster.sh --schedulers 5
java -jar predisched-client/target/predisched-client.jar admin leader
# stop scheduler 5, then:
java -jar predisched-client/target/predisched-client.jar admin leader
java -jar predisched-benchmark/target/predisched-benchmark.jar election-compare --runs 10
```

`election-compare` measures message count and time-to-leader for Bully and Ring with 3 and 5 nodes and
writes `results/election-compare.csv`.

### Docs

`docs/components/leadership.md`; election row in `docs/LAB-COVERAGE.md` with the comparison table.
