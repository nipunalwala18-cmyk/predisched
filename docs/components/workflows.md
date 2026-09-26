# Workflows: task DAGs (F1)

A client can submit a workflow: a set of tasks where some depend on others. The scheduler runs it in
dependency order, passes a parent's result into a child's input when asked, and cancels everything
downstream of a failure.

Built by `spec/prompts/09-workflows-auth.md`. Spec sections: §4 FR22, §6 F1, §7.2 (`WORKFLOW_TASK`),
§10.1.

## Contract

- `TaskRequest.depends_on` lists the task ids that must complete first. It is only accepted inside
  a workflow; a plain `SubmitTask` with `depends_on` is refused.
- `SubmitWorkflow(WorkflowRequest)` takes a workflow id and its tasks, and is accepted as a whole
  or not at all. `GetWorkflowStatus` returns every task in topological order.
- `TaskStatus.BLOCKED` (5): a task waiting on its parents. `TaskStateMachine` adds
  `BLOCKED → QUEUED` (every parent completed) and `BLOCKED → CANCELLED` (a parent failed or was
  cancelled). Nothing moves *into* BLOCKED: a task starts there (`TaskRecord.asBlocked()`).
- `TaskRecord` gains `workflowId` and `clientId`; both are replicated (`TaskRecordProto` 16, 17).

## How it runs (`WorkflowManager`)

1. **Validate** the whole workflow: non-empty and unused id, at most 1000 tasks, unique task ids,
   every `depends_on` inside the workflow and not the task itself, every `${x.result}` naming one of
   the task's own parents, each task valid by the normal rules, and no cycle. Cycles are found with
   Kahn's algorithm (repeatedly take a task with no unfinished parents; whatever is left over sits on
   a cycle), which also gives the topological order. No graph library was needed.
2. **Store** every task, roots as QUEUED and the rest BLOCKED, *then* queue the roots, so a fast root
   can never finish before its children exist.
3. **Release**: `RetryCoordinator` now tells listeners whenever a task reaches a terminal state. On
   COMPLETED, each child's count of unfinished parents (an `AtomicInteger`) goes down; the call that
   takes it to zero substitutes `${parent.result}` in the child's input, re-validates it, and moves the
   child to QUEUED. If the substituted input is invalid, the child is cancelled with the reason.
4. **Cancel**: on FAILED or CANCELLED, every child still BLOCKED becomes CANCELLED with
   `UPSTREAM_FAILED: <parent> <status>`, and that cancellation cascades the same way to its children.
   Cancelling a BLOCKED task through `CancelTask` starts the same cascade.

Every status change goes through the store's atomic `update`, and a release or cancel only acts if
it was the call that changed the status, so two parents finishing together release a child once.

## `WORKFLOW_TASK`

`WORKFLOW_TASK` with input `dag=<path>` (a file the scheduler can read) is expanded into a workflow
whose id is the task's id. The task itself is added as the last node, depending on every other; it
never goes to a worker, and completes the moment it is released. Its status is therefore the
workflow's: COMPLETED when everything is, CANCELLED if anything failed.

## Workflow files

`workloads/dags/map-reduce.json`: four `CPU_TASK` maps and one `SORT_TASK` reduce that depends on all
four.

```json
{"tasks": [
  {"id": "map-1", "type": "CPU_TASK", "input": "n=3000000", "priority": 5},
  ...
  {"id": "reduce", "type": "SORT_TASK", "input": "n=400000, type=random", "priority": 5,
   "depends_on": ["map-1", "map-2", "map-3", "map-4"]}
]}
```

Ids are local to the file. `WorkflowDag.toRequest` prefixes every id, every `depends_on` and every
`${id.result}` with the workflow id (`wf-255bee98.map-1`), so one file can run any number of times.
The file is read with SnakeYAML, which parses JSON as YAML flow style.

## How to run and demo

```bash
java -jar predisched-client/target/predisched-client.jar --api-key dev-key-1 workflow submit workloads/dags/map-reduce.json
java -jar predisched-client/target/predisched-client.jar --api-key dev-key-1 workflow status <workflowId>
java -jar predisched-client/target/predisched-client.jar submit --type WORKFLOW_TASK --input dag=workloads/dags/map-reduce.json --priority 5
```

## Real output

Scheduler and worker started with `configs/secure.yaml` (auth on), one worker:

```
$ predisched --api-key dev-key-1 workflow submit workloads/dags/map-reduce.json
accepted=true workflow_id=wf-255bee98 message='workflow accepted: 5 tasks'
$ predisched --api-key dev-key-1 workflow status wf-255bee98
workflow wf-255bee98:
  wf-255bee98.map-1            COMPLETED worker-1  primes_below_3000000=216816
  wf-255bee98.map-2            COMPLETED worker-1  primes_below_3000000=216816
  wf-255bee98.map-3            COMPLETED worker-1  primes_below_3000000=216816
  wf-255bee98.map-4            COMPLETED worker-1  primes_below_3000000=216816
  wf-255bee98.reduce           COMPLETED worker-1  n=400000 type=random sorted=true sum=-39814144426 first=-214...
```

## Tests

`WorkflowManagerTest`: a diamond DAG releases a, then b and c, then d only after both; a cycle is
rejected naming `[x, y, z]` while an independent task is not blamed; unknown parents and references to
non-parents are rejected; a failed parent cancels all three descendants with `UPSTREAM_FAILED` and an
independent branch keeps running; `${wf3.size.result}` becomes `ms=42`; a substitution that breaks the
input cancels the child. `WorkflowIntegrationTest`: `map-reduce.json` through the real service,
dispatcher and a fake worker completes with the reduce started after every map finished;
a `WORKFLOW_TASK` expands the same file and completes with it; `depends_on` outside a workflow is
refused.

Not yet: workflow state lives in memory on the leader (prompt 10 hands it over on failover, prompt 11
persists it); the DAG view on the dashboard is prompt 23.
