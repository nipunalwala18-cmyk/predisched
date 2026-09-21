# Lab coverage

| Exp | Topic | Code | Demo command | Measured result |
| --- | --- | --- | --- | --- |
| 1 | Client-server communication using RPC / RMI | `proto/task.proto`, `proto/worker.proto`, `predisched-scheduler/src/main/java/com/predisched/scheduler/`, `predisched-worker/src/main/java/com/predisched/worker/`, `predisched-client/src/main/java/com/predisched/client/` | `java -jar predisched-client/target/predisched-client.jar submit --type CPU_TASK --input n=2000000 --priority 5` then `watch <id>` (scheduler on 51051, worker on 51061) | `CPU_TASK n=2000000` COMPLETED on worker-1 in 19 ms; `MATRIX_TASK size=200` in 45 ms; invalid submit rejected with every validation error |
| 2 | Multithreading in a distributed system | not started | not started | not started |
| 3 | Clock synchronization (logical / physical) | not started | not started | not started |
| 4 | Bully and Ring election | not started | not started | not started |
| 5 | Data consistency and replication models | not started | not started | not started |
| 6 | Load balancing | not started | not started | not started |
| 7 | MapReduce with Hadoop / Spark | not started | not started | not started |
| 8 | Fault tolerance with primary-backup replication | not started | not started | not started |
| 9 | MPI collectives: Broadcast, Scatter, Gather | not started | not started | not started |
| 10 | Parallel matrix multiplication using MPI | not started | not started | not started |
