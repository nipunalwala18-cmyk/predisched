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
