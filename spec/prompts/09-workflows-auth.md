# Prompt 09: Task DAG workflows and client authentication with rate limits (F1, F9)

## Goal

Clients can submit a workflow of dependent tasks that runs in topological order, and every client call
is authenticated with an API key (or JWT) and held to a per-client rate limit and quota.

## Read first

- Spec §4 FR22 and FR30, §6 Tier 1 (F1, F9), §7.2 (`WORKFLOW_TASK`), §10.1

## Build

1. **Protos**: add `repeated string depends_on` to `TaskRequest`. Add
   `SchedulerService.SubmitWorkflow(WorkflowRequest) returns (TaskResponse)` where `WorkflowRequest`
   holds a workflow id and a list of `TaskRequest`s. Add `BLOCKED` to `TaskStatus` (a task waiting on
   parents) with the next free number, and extend `TaskStateMachine`: `BLOCKED → QUEUED` when all
   parents complete, `BLOCKED → CANCELLED` when any parent fails or is cancelled.
2. **`predisched-scheduler`**
   - `WorkflowManager`: validates the DAG (unknown ids, cycles, size limit) with a plain Kahn's
     algorithm (no JGraphT unless it is clearly simpler), stores children per parent, and releases
     children into the queue when their last parent completes. Parent failure cancels descendants
     with reason `UPSTREAM_FAILED`.
   - `WORKFLOW_TASK` with input `dag=<path>` loads a JSON DAG file (see `workloads/dags/`) and expands
     it through `SubmitWorkflow`. Ship `workloads/dags/map-reduce.json`: 4 `CPU_TASK` maps → 1
     `SORT_TASK` reduce.
   - A parent's result can be passed to a child's input with `${<taskId>.result}` substitution.
3. **Auth** (`predisched-common` for the interceptor, scheduler for enforcement)
   - `AuthInterceptor` (server side): reads `authorization: ApiKey <key>` or `Bearer <jwt>` metadata.
     Keys and client ids come from `configs/clients.yaml` (keys stored as SHA-256 hashes). JWTs are
     HS256 via `jjwt`, secret from an env var. Missing or bad credentials → `UNAUTHENTICATED`.
   - `RateLimiter`: token bucket per client (rate and burst from config); exceeding it →
     `RESOURCE_EXHAUSTED`. `Quota`: max queued tasks per client.
   - Scheduler-to-scheduler and scheduler-to-worker calls use a node credential so internal traffic
     is not rate limited. `auth.enabled: false` keeps local dev simple.
   - The client id is stored on `TaskRecord` (used for fair share later).
   - Optional TLS for gRPC: a `tls.enabled` flag and cert paths; document it, off by default.
4. **CLI**: `--api-key` option / `PREDISCHED_API_KEY` env var; `workflow submit <dag.json>`;
   `workflow status <workflowId>` prints each node's state.

## Tests

- `WorkflowManagerTest`: topological release order, a cycle rejected, a failed parent cancels all
  descendants, result substitution.
- `AuthInterceptorTest`: valid key, wrong key, missing key, expired JWT.
- `RateLimiterTest` with an injected clock: burst allowed, then limited, then refilled.
- Integration: the `map-reduce.json` workflow completes with the reduce task last.

## Acceptance checks

```bash
java -jar predisched-client/target/predisched-client.jar --api-key dev-key-1 workflow submit workloads/dags/map-reduce.json
```
```bash
java -jar predisched-client/target/predisched-client.jar --api-key dev-key-1 workflow status <workflowId>
```
```bash
java -jar predisched-client/target/predisched-client.jar --api-key wrong submit --type SLEEP_TASK --input ms=10 --priority 5
```

Also fire 50 submits in one second with a key limited to 10/s and paste how many were refused.

## Docs and commit

- `docs/components/workflows.md` and `docs/components/auth.md`.
- Commit: `Workflows: DAG tasks run in dependency order; clients authenticate and are rate limited`
