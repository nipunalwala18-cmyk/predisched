# PrediSched build spec

This folder is the source of truth for building PrediSched. It holds the specification, the rules
every coding agent follows, and the numbered build prompts that turn the spec into one working product.

| File | What it is |
| --- | --- |
| [PROJECT-CONTEXT.md](PROJECT-CONTEXT.md) | The full project specification. Prompts cite it as "spec §N". |
| [RULES.md](RULES.md) | Non-negotiable engineering rules, pinned versions, ports, definition of done. |
| [prompts/](prompts/) | 25 build prompts, `00` to `24`, run strictly in order. |

## How to run the prompts

1. Open a fresh agent session in the repo root.
2. Paste: `Read spec/RULES.md, then run spec/prompts/NN-<name>.md.`
3. The agent reads the spec sections the prompt names, builds, runs the acceptance checks, updates
   the docs, and makes one commit.
4. Review the report and the commit, then run the next prompt.

Do not skip ahead. Each prompt assumes every earlier prompt is committed and `mvn -q verify` is green.
If a prompt's acceptance check fails, fix it in the same prompt before moving on.

## Build order

| # | Prompt | Lab topic / features | Phase |
| --- | --- | --- | --- |
| 00 | [Bootstrap](prompts/00-bootstrap.md) | Repo skeleton, Maven, CI | Foundation |
| 01 | [Task API over gRPC](prompts/01-task-api-rpc.md) | Exp 1 RPC | Foundation |
| 02 | [Workers and thread pools](prompts/02-workers-multithreading.md) | Exp 2 Multithreading | Foundation |
| 03 | [Clocks and tracing](prompts/03-clocks-and-tracing.md) | Exp 3 Clock sync, F10 | Foundation |
| 04 | [Queue discipline](prompts/04-queue-discipline.md) | F2, F4, F5 | Foundation |
| 05 | [Task catalogue and workload generator](prompts/05-task-catalogue-workloads.md) | Spec §7, F8 | Foundation |
| 06 | [Leader election](prompts/06-leader-election.md) | Exp 4 Bully / Ring | Coordination |
| 07 | [Replication and consistency](prompts/07-replication-consistency.md) | Exp 5 | Coordination |
| 08 | [Load-balancing strategies](prompts/08-load-balancing.md) | Exp 6 | Distribution |
| 09 | [Workflows and client auth](prompts/09-workflows-auth.md) | F1, F9 | Distribution |
| 10 | [Primary-backup fault tolerance](prompts/10-primary-backup.md) | Exp 8 | Distribution |
| 11 | [PostgreSQL persistence and result cache](prompts/11-persistence-cache.md) | Spec §14, F6, F3 | Distribution |
| 12 | [Spark MapReduce](prompts/12-spark-mapreduce.md) | Exp 7 | Distribution |
| 13 | [MPI collectives](prompts/13-mpi-collectives.md) | Exp 9 | Parallelism |
| 14 | [MPI matrix multiplication](prompts/14-mpi-matrix.md) | Exp 10 | Parallelism |
| 15 | [Dataset collection](prompts/15-dataset-collection.md) | Spec §12.1 | Data |
| 16 | [ML models](prompts/16-ml-models.md) | Spec §12.2 | ML |
| 17 | [Prediction server](prompts/17-prediction-server.md) | Spec §12.3 | ML |
| 18 | [Predictive strategy](prompts/18-predictive-strategy.md) | Spec §12.4, F13 | Integration |
| 19 | [Speculative execution and chaos](prompts/19-speculation-chaos.md) | F11, F16 | Integration |
| 20 | [Benchmark harness and report](prompts/20-benchmark-report.md) | Spec §13, F17, F18 | Evaluation |
| 21 | [Model lifecycle and auto-scaling](prompts/21-model-lifecycle-autoscaling.md) | F12, F14, F15 | Evaluation |
| 22 | [Dashboard API](prompts/22-dashboard-api.md) | Spec §15.10, F7 | Presentation |
| 23 | [Dashboard UI](prompts/23-dashboard-ui.md) | Spec §15 | Presentation |
| 24 | [Deployment and final report](prompts/24-deployment-final.md) | Spec §16, demo | Presentation |

Prompts 00–20 plus 22–24 cover all ten lab experiments and the spec's minimum feature set
(F1–F8, F11, F13, F16, F18). Prompt 21 and the Tier 3 features (F19–F25) are stretch work.

## Prompt format

Every prompt has the same sections:

- **Goal**: what works when the prompt is done, in one or two sentences.
- **Read first**: the spec sections to read before writing code.
- **Build**: the concrete work, module by module.
- **Tests**: the tests that must exist and pass.
- **Acceptance checks**: commands to run; their real output goes in the report.
- **Docs and commit**: the component doc, the lab-coverage entry and the commit message.
