# Lab topic coverage

Each distributed-systems lab topic is implemented as a working part of PrediSched, not as a separate
program. This table is the map. The evidence columns are filled in by the prompt that builds each
part, with real commands and real measured output.

| # | Lab topic | Where it lives in the product | Why the product needs it | Built in | Demo command | Measured result |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Client-server communication using RPC / RMI | `proto/`, `SchedulerService`, `WorkerService`, `PredictionService` (Java ↔ Java and Java ↔ Python) | Every hop between client, schedulers, workers and the model server | Prompts 01, 13 | pending | pending |
| 2 | Multithreading in a distributed system | `predisched-worker` thread pools; scheduler dispatcher and concurrent submit path | Workers run many tasks at once; the scheduler serves many clients | Prompt 02 | pending | pending |
| 3 | Clock synchronization (logical and physical) | `common/clock`: Lamport interceptors on every gRPC call; Berkeley daemon on the leader; Cristian alternative | Causal event ordering, last-writer-wins, trustworthy timestamps in the ML dataset | Prompt 04 | pending | pending |
| 4 | Bully and Ring election | `predisched-election` | Choosing the primary scheduler; recovering when it dies | Prompt 05 | pending | pending |
| 5 | Data consistency and replication models | `predisched-replication`: quorum (strong) and eventual stores, read-your-writes | Task state must survive the loss of a scheduler | Prompt 06 | pending | pending |
| 6 | Load balancing | `predisched-scheduler/strategy`: Round Robin, Random, Least Loaded, Resource-Aware, Predictive | Placing tasks; the reactive strategies are the predictive scheduler's baselines | Prompts 03, 14, 15 | pending | pending |
| 7 | MapReduce with Hadoop / Spark | `spark/`: RDD map/reduce over execution logs | Operational statistics and the ML training dataset | Prompt 11 | pending | pending |
| 8 | Fault tolerance with primary-backup replication | `predisched-fault`: ordered replication, promotion, rejoin, worker failure recovery | No accepted task lost when a scheduler or worker dies | Prompt 07 | pending | pending |
| 9 | MPI collectives: Broadcast, Scatter, Gather | `mpi/predisched_mpi/collectives.py` batch engine | Running a batch of tasks across ranks | Prompt 09 | pending | pending |
| 10 | Parallel matrix multiplication using MPI | `mpi/predisched_mpi/matmul.py` + `MpiMatrixTaskExecutor` | MPI backend for `MATRIX_TASK`; heavy benchmark workload | Prompt 09 | pending | pending |
