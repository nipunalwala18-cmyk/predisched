# Leader election (lab Exp 4: Bully and Ring)

Five scheduler nodes, election ids 1–5, elect a coordinator. Only the leader accepts `SubmitTask` and
dispatches; followers answer with `accepted=false` and `not the leader; leader=<id>`. When the leader
dies the followers notice, elect a new one, and exactly one leader comes out. Bully is the default;
Ring is selected by `election.algorithm: ring` or `--election-algorithm ring`.

Built by `spec/prompts/06-leader-election.md`. Spec sections: §4 FR9, §5 (Availability), §10.3,
§11 Exp 4, §17 (Election row).

## Pieces

| Class (`predisched-election`) | Role |
| --- | --- |
| `ElectionAlgorithm` | The interface: `start()`, `startElection()`, `leaderFailed()`, `onElection`, `onRingPass`, `onCoordinator`, `leader()`, a leader-change listener. `create(name, …)` picks the implementation from config. |
| `BullyElection`, `RingElection` | The two algorithms, sharing leader bookkeeping, stats and threads in `AbstractElection`. |
| `ClusterView` | Node id → gRPC channel for every peer (from `election.peers`), opened lazily, Lamport interceptor on every call. |
| `LeaderMonitor` | Followers `Ping` the leader every `pingIntervalMs`; `pingMisses` failures in a row start an election. |
| `ElectionStats` | `AtomicLong` counters of messages sent per type, and the time from detection to learning the new leader. |
| `ElectionServiceImpl` | The gRPC service from `proto/election.proto`. `Ping` replies with `node=3 leader=5 algorithm=bully`. |
| `InProcessElectionCluster` | N real nodes over in-process gRPC; a killed node's server shuts down. Used by the tests and `election-compare`. |

In `predisched-scheduler`, `SchedulerNode` builds the election from config and implements
`Leadership`, which `SchedulerServiceImpl` asks before accepting a task. A scheduler with no
`election.peers` (for example `configs/local.yaml`) runs alone and is always its own leader, so every
earlier demo still works unchanged. The Berkeley daemon runs on every node but is given peers only
on the leader, so exactly one node corrects the clocks. Workers register with every scheduler in
`election.peers`, so a newly elected leader already knows them and can dispatch at once.

## Bully

```
 node 1 notices leader 5 is gone               (5 is dead: its calls fail)
   1 --Election--> 2, 3, 4, 5      2, 3, 4 answer OK; 1 now waits for a Coordinator
   2 --Election--> 3, 4, 5         3, 4 answer OK
   3 --Election--> 4, 5            4 answers OK
   4 --Election--> 5               no answer within timeoutMs: 4 is the highest alive
   4 --Coordinator(4)--> 1, 2, 3, 5
```

A node that gets `Election` from a lower id answers OK (the successful `Ack`) and runs its own
election. No OK within `timeoutMs` means declare yourself leader and broadcast `Coordinator`. An OK
followed by no `Coordinator` within `timeoutMs` means the higher node died mid-election, so try again.
A `Coordinator` from a *lower* id than yourself starts an election: you are alive and outrank it.

## Ring

```
 ring order 1 -> 2 -> 3 -> 4 -> 5 -> 1, node 5 dead, node 2 initiates
   2 --RingPass[2]--> 3
   3 --RingPass[2,3]--> 4
   4 --RingPass[2,3,4]--> 5          UNAVAILABLE: skip to the next successor
   4 --RingPass[2,3,4]--> 1
   1 --RingPass[2,3,4,1]--> 2        back at the initiator: max = 4 is the leader
   2 --Coordinator(4, origin 2)--> 3 --> 4 --> (5, skip) --> 1
                                     1 stops: its next hop would be the origin, 2
```

Each node appends its id and forwards to the first successor that answers, which is how a dead node
is skipped. `CoordinatorMsg` carries `origin_id` (a field added to spec §10.3) so the announcement
goes round exactly once. Handlers acknowledge immediately and forward on the node's election thread,
so a round is a chain of short calls, not n nested ones. If a round never comes back (a node died
holding it), the initiator starts another after `timeoutMs × n`.

## Timeouts (`configs/cluster.yaml`)

| Key | Value | Meaning |
| --- | --- | --- |
| `election.algorithm` | `bully` | `bully` or `ring` |
| `election.timeoutMs` | 1000 | wait for OKs, then for the winner's Coordinator; also the per-call deadline |
| `election.pingIntervalMs` | 500 | how often a follower pings the leader |
| `election.pingMisses` | 3 | consecutive failed pings before an election starts |

Detection therefore takes about 1–1.5 s on its own: that, not the election, dominates failover.

## Threading

Election logic for a node runs on one `election-<id>` thread, so its own steps happen in order;
peer calls that may block go to a cached `election-rpc-<id>` pool, so one dead peer cannot stall the
Election messages to the others. Leader, running flag and counters are atomics; Bully's
"Coordinator arrived" signal is a counter guarded by its own monitor. The monitor's miss counter is
confined to the single monitor thread. Every election log line carries node id and Lamport time, and
`ELECTION_START` / `LEADER_ELECTED` events go to `logs/<node>.jsonl`.

## How to run and demo

```bash
mvn -q install -DskipTests
scripts/start-cluster.sh                 # or: scripts\start-cluster.ps1 [-Algorithm ring]
java -jar predisched-client/target/predisched-client.jar cluster leader
scripts/stop-node.sh scheduler 5         # or: scripts\stop-node.ps1 scheduler 5
# about 5 s later
java -jar predisched-client/target/predisched-client.jar cluster leader
scripts/stop-node.sh all
scripts/start-cluster.sh ring            # the same with Ring
java -jar predisched-benchmark/target/predisched-benchmark.jar election-compare
```

`start-cluster` starts schedulers on 51051–51055 and workers on 51061–51063 with output in
`logs/<node>.out`. On Windows it creates the processes through WMI, so the script returns at once
even when its output is piped; `stop-node` kills the whole process tree, like a crash.

## Real output

Bully, live cluster: a follower refuses a submit, the leader takes it, then the leader is killed.

```
$ predisched --port 51051 submit --type CPU_TASK --input n=200000 --priority 5
accepted=false task_id=task-ae06c267 trace=9bc7ad1a lamport=0 message='not the leader; leader=5'
$ predisched --port 51055 submit --type CPU_TASK --input n=200000 --priority 5
accepted=true task_id=task-9c230afe trace=fbe94702 lamport=5070 message='queued'
--- kill scheduler 5 at 22:46:28.378
$ predisched cluster leader            (at 22:46:34)
scheduler 1 (localhost:51051): node=1 leader=4 algorithm=bully
scheduler 2 (localhost:51052): node=2 leader=4 algorithm=bully
scheduler 3 (localhost:51053): node=3 leader=4 algorithm=bully
scheduler 4 (localhost:51054): node=4 leader=4 algorithm=bully
scheduler 5 (localhost:51055): unreachable (UNAVAILABLE)
```

Election log lines from the four survivors (node 4 then ran a `HASH_TASK` on worker-1 in 128 ms):

```
22:46:30.069 WARN  [scheduler-1 L=5155 ] Leader 5 stopped answering; node 1 starts a bully election
22:46:30.074 INFO  [scheduler-2 L=5160 ] Node 2 starts a bully election
22:46:30.074 INFO  [scheduler-3 L=5160 ] Node 3 starts a bully election
22:46:30.080 INFO  [scheduler-4 L=5162 ] Node 4 starts a bully election
22:46:30.087 INFO  [scheduler-4 L=5162 ] Leader is now 4 (bully): elected in 5 ms
22:46:30.089 INFO  [scheduler-4 L=5162 ] This scheduler (node 4) is now the leader: accepting tasks
22:46:30.094 INFO  [scheduler-1 L=5171 ] Leader is now 4 (bully): elected in 24 ms
```

Ring, live cluster, killed at 22:48:59.010:

```
22:49:01.474 WARN  [scheduler-2 L=379 ] Leader 5 stopped answering; node 2 starts a ring election
22:49:01.485 INFO  [scheduler-4 L=389 ] Ring successor 5 of node 4 is unreachable (UNAVAILABLE); skipping it
22:49:01.500 INFO  [scheduler-2 L=396 ] Ring pass came back to node 2 through [2, 3, 4, 1]: leader is 4
22:49:01.501 INFO  [scheduler-2 L=396 ] Leader is now 4 (ring): elected in 24 ms
22:49:01.555 INFO  [scheduler-4 L=411 ] This scheduler (node 4) is now the leader: accepting tasks

$ predisched cluster leader            (at 22:49:06)
scheduler 1 (localhost:51051): node=1 leader=4 algorithm=ring
scheduler 2 (localhost:51052): node=2 leader=4 algorithm=ring
scheduler 3 (localhost:51053): node=3 leader=4 algorithm=ring
scheduler 4 (localhost:51054): node=4 leader=4 algorithm=ring
scheduler 5 (localhost:51055): unreachable (UNAVAILABLE)
```

From kill to agreement: 1.7 s with Bully and 2.5 s with Ring in these two live runs. Almost all of it
is failure detection: on Windows a call to a closed localhost port can take most of its 500 ms ping
deadline to fail, so three misses cost 1.5–2.5 s. The elections themselves took 5–59 ms.

## Measured comparison

`election-compare` runs five in-process nodes with the timeouts above, elects 5, kills it, and
measures; 5 runs per row, all in `results/exp4-election.csv`. `messages` counts Election, OK,
Coordinator and RingPass sends by every node (pings excluded), including failed sends to the dead
node. `failover_ms` runs from the kill (or, for `lowest_detects`, from node 1 being told) until all
survivors agree; `election_ms` is the longest detection-to-leader time any node saw.

| Algorithm | Scenario | Mean messages | Mean failover ms | Mean election ms |
| --- | --- | --- | --- | --- |
| Bully | lowest_detects (only node 1 notices) | 20.0 | 8 | 3 |
| Ring | lowest_detects (only node 1 notices) | 9.0 | 6 | 2 |
| Bully | all_detect (every follower pings, as live) | 24.0 | 1184 | 7 |
| Ring | all_detect (every follower pings, as live) | 30.6 | 1126 | 2 |

What the numbers say:

- **One detector (the textbook case):** Bully's worst case, started by the lowest id, cost 20
  messages against Ring's 9 for the same failover: O(n²) against O(n). `BullyElectionTest` asserts
  at least n(n−1)/2 = 10 and `RingElectionTest` at most 2n = 10.
- **Every follower detecting (what the live cluster does):** Ring used *more* messages than Bully,
  30.6 against 24. Each follower that notices starts its own round, and each round costs about 2n
  regardless of who started it, while Bully's extra elections are cheap once node 4 has answered
  them. Ring is O(n) per round, not per failure.
- **Time:** both elect in single-digit milliseconds once the failure is detected. Failover time is
  set by `pingIntervalMs × pingMisses`, not by the algorithm.
