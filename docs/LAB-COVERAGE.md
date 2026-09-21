# Lab coverage

| Exp | Topic | Code | Demo command | Measured result |
| --- | --- | --- | --- | --- |
| 1 | Client-server communication using RPC / RMI | `proto/task.proto`, `proto/worker.proto`, `predisched-scheduler/src/main/java/com/predisched/scheduler/`, `predisched-worker/src/main/java/com/predisched/worker/`, `predisched-client/src/main/java/com/predisched/client/` | `java -jar predisched-client/target/predisched-client.jar submit --type CPU_TASK --input n=2000000 --priority 5` then `watch <id>` (scheduler on 51051, worker on 51061) | `CPU_TASK n=2000000` COMPLETED on worker-1 in 19 ms; `MATRIX_TASK size=200` in 45 ms; invalid submit rejected with every validation error |
| 2 | Multithreading in a distributed system | `predisched-worker/src/main/java/com/predisched/worker/ExecutionEngine.java`, `WorkerMetrics.java`, `RegistrationClient.java`, `predisched-scheduler/src/main/java/com/predisched/scheduler/WorkerRegistry.java`, `Dispatcher.java` | `java -jar predisched-benchmark/target/predisched-benchmark.jar pool-size`; or run a worker with `--pool-size 4` and submit 8 sleep tasks at once | Median of 3 reps, 40 x CPU_TASK n=20000000, 8 cores: pool 1 = 5.68 tasks/s, pool 2 = 6.42 (1.13x), pool 4 = 8.22 (1.45x), pool 8 = 8.35 (1.47x); `results/exp2-pool-size.csv` |
| 3 | Clock synchronization (logical / physical) | `predisched-common/src/main/java/com/predisched/common/time/` (`LamportClock`, `PhysicalClock`, `BerkeleyDaemon`, `CristianClient`, `ClockServiceImpl`), `common/obs/LamportInterceptors.java`, `common/obs/EventLog.java` | Start a scheduler and two workers with `--clock-offset -300` / `--clock-offset 500`, submit a task, then `python scripts/merge-events.py logs`; Cristian with `--config configs/cristian.yaml` | Berkeley: spread 807 ms -> 13 ms -> 2 ms over three rounds (measured offsets -294 / +513 ms). Cristian: a worker 450 ms behind corrected to 0 ms in one round (rtt 36 ms). Lamport order fixes an event that physical order shows 11 ms before its own cause |
| 4 | Bully and Ring election | not started | not started | not started |
| 5 | Data consistency and replication models | not started | not started | not started |
| 6 | Load balancing | not started | not started | not started |
| 7 | MapReduce with Hadoop / Spark | not started | not started | not started |
| 8 | Fault tolerance with primary-backup replication | not started | not started | not started |
| 9 | MPI collectives: Broadcast, Scatter, Gather | not started | not started | not started |
| 10 | Parallel matrix multiplication using MPI | not started | not started | not started |
