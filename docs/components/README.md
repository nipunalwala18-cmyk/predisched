# Components

Each component gets one file in this folder, named `<name>.md`.

Every file follows the definition of done in `spec/RULES.md`: what the component does, its design
choices, how to run and demo it, and real output from the acceptance checks.

| File | Component | Prompt |
| --- | --- | --- |
| [task-api.md](task-api.md) | Task API over gRPC: submit, track, cancel | 01 |
| [worker.md](worker.md) | Workers, thread pools, registration, heartbeats | 02 |
| [clocks.md](clocks.md) | Lamport clocks, Berkeley and Cristian sync, event log | 03 |
| [queue.md](queue.md) | Priority ageing, retries, dead-letter queue, timeouts | 04 |
| [tasks-and-workloads.md](tasks-and-workloads.md) | Task catalogue, executors, workload generator and replay | 05 |
| [election.md](election.md) | Bully and Ring leader election across 5 schedulers | 06 |
| [replication.md](replication.md) | Replicated task state: strong quorum and eventual consistency | 07 |
| [strategies.md](strategies.md) | Round Robin, Random, Least Loaded, Resource-Aware worker selection | 08 |
| [workflows.md](workflows.md) | Task DAGs: dependencies, release order, result passing, WORKFLOW_TASK | 09 |
| [auth.md](auth.md) | API keys, JWT, node credentials, rate limits, quotas, optional TLS | 09 |
| [fault-tolerance.md](fault-tolerance.md) | Primary-backup failover, in-doubt dispatches, worker failure detection, client failover | 10 |
| [storage.md](storage.md) | PostgreSQL history, result cache, deadlines and SLA report, DB_QUERY_TASK | 11 |
| [spark.md](spark.md) | Spark MapReduce over execution history, ML features, MAPREDUCE_TASK | 12 |
| [mpi.md](mpi.md) | MPI collectives (Broadcast, Scatter, Gather), parallel matrix multiplication with speedup, MATRIX_TASK mode=mpi | 13, 14 |
| [dataset.md](dataset.md) | Dataset campaign on heterogeneous workers, future-looking labels, leak-free features, dataset card | 15 |
| [ml-models.md](ml-models.md) | Execution-time, queue-forecast and overload models: features, time split, candidates, metrics, ML_INFER_TASK | 16 |
| [prediction-server.md](prediction-server.md) | gRPC PredictionService: batched per-worker predictions, hot reload, prediction log; Java client with deadline and circuit breaker | 17 |
| [predictive-strategy.md](predictive-strategy.md) | Predictive scheduling: cost function, fallback to least loaded, explain, live MAE, lambda tuning | 18 |
