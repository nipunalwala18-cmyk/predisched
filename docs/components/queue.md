# Queue discipline (F2, F4, F5)

The scheduler's queue now has rules: high-priority work goes first but nothing starves, failed tasks
retry with exponential backoff and then get parked instead of forgotten, and a task that overstays
its deadline is stopped on the worker.

Built by `spec/prompts/04-queue-discipline.md`. Spec sections: §4 (FR23, FR25, FR26), §6 Tier 1
(F2, F4, F5), §3.3.

## Priority with ageing (F2, FR23)

`AgeingPriorityQueue` computes a task's priority when it is taken, not when it is added:

```
effective = min(maxPriority, priority + ageingPerSecond * secondsWaiting)
```

Nothing re-sorts on a timer; `take()` scans the queued tasks and picks the highest effective
priority, breaking ties in favour of whoever waited longer (so equal priorities stay FIFO). The scan
is O(n) per dispatch, which for the hundreds of tasks this system queues is cheaper than maintaining
a heap whose keys change with the clock. One `ReentrantLock` with a `Condition` guards the list.

`TaskQueue` is an interface, so the ordering policy can be swapped (per-user fair share, spec §6
F19) without touching dispatch.

### Why the dispatcher now holds work back

The first live run of this feature showed ageing doing nothing at all: the queue was always empty
and every task reported `waited 0 ms`. The cause was not the queue. The dispatcher sent tasks to a
worker as fast as clients submitted them, and a worker accepts far more than it can run at once (its
pool queue holds 100), so the scheduler's queue drained into the worker's and the scheduler's
ordering decided nothing.

`Dispatcher.chooseWorker()` now only picks a worker with spare capacity:

```
outstanding on that worker < poolSize * outstandingPerWorkerFactor   (default 2.0)
```

The count comes from the scheduler's own in-flight map (`RunningTasks`), so it is exact and
immediate rather than up to one heartbeat old. Above 1.0 keeps the worker's pool fed; too high
reproduces the original problem.

## Retries, backoff and the dead-letter queue (F4, FR25)

Every dispatch is an **attempt**, recorded on the task as a `TaskAttempt` with its outcome
(`SUCCEEDED`, `FAILED`, `TIMED_OUT`, `REJECTED`, `WORKER_LOST`), worker, reason and timings. The
dispatcher records how the attempt ended and hands it to `RetryCoordinator`, which owns the
decision:

- **`REJECTED`** (the worker's queue was full) does not spend a retry: the worker never ran the
  task, so it goes straight back to the queue for a different worker.
- **A real failure** schedules a retry after `RetryPolicy.delayMsAfter(attempt)`:
  `base * 2^(attempt-1)`, capped at `retryMaxDelayMs`, with jitter from a **seeded** `Random`, so a
  replayed workload retries at the same moments (rule 8).
- **Out of retries** parks the task in the `DeadLetterQueue` and marks it `FAILED`.

Parking happens *before* the status flips to `FAILED`. Written the other way round, a client that
reacted to `FAILED` could list the dead letters and not find the task yet; the integration test
caught exactly that race.

`dlq retry` puts a parked task back. Since `FAILED` is terminal, the record is replaced with a fresh
`QUEUED` one that keeps the original request (`TaskStore.replace`, which exists only for this).

## Timeouts (F5, FR26)

`TimeoutWatcher` scans `RunningTasks` every `timeoutCheckMs` and, for anything past its deadline,
calls `WorkerService.CancelExecution` on the worker that holds it. It deliberately does **not**
decide the task's fate: it marks the attempt timed out, the dispatcher's own call then returns, and
the attempt is recorded in one place as `TIMED_OUT`. Retries apply to timeouts like any other
failure.

On the worker, `ExecutionEngine` keeps the pool `Future` plus the pending reply for each task.
Cancellation interrupts a running task and answers the waiting gRPC call; a task cancelled before it
started never runs its body, so the engine answers that call itself. Long-running executors check
`Thread.isInterrupted()` (the prime sieve every 65,536 numbers), or the cancellation would have no
effect until the task finished on its own.

## Configuration

```yaml
scheduler:
  outstandingPerWorkerFactor: 2.0   # tasks kept on one worker, as a multiple of its pool size
queue:
  ageingPerSecond: 0.1              # priority gained per second waiting
  maxPriority: 10
  maxRetries: 3
  retryBaseDelayMs: 200
  retryMaxDelayMs: 30000
  retryJitter: 0.2
  defaultTimeoutMs: 0               # 0: only tasks that ask for one time out
  timeoutCheckMs: 250
  seed: 42                          # retry jitter, so replays are reproducible
```

Per task: `--timeout <ms>` and `--max-retries <n>`. `--queue-ageing` on the scheduler makes the
starvation demo run in seconds instead of minutes.

## Real output

### Retries and the dead-letter queue

```bash
predisched submit --type SLEEP_TASK --input "ms=200, failRate=1.0, seed=1" --priority 5
```

```
Attempt 1 of task-be875d8c failed (injected failure); retrying in 218 ms (1 of 3 retries used)
Attempt 2 of task-be875d8c failed (injected failure); retrying in 429 ms (2 of 3 retries used)
Attempt 3 of task-be875d8c failed (injected failure); retrying in 739 ms (3 of 3 retries used)
```

The delays double (200 → 400 → 800 ms nominal) with jitter inside ±20 %. Then:

```
task_id=task-be875d8c status=FAILED attempts=4 result='error: injected failure (failRate=1.00, draw=0.731)'
  attempt #1 FAILED on worker-1 (injected failure) 213ms
  attempt #2 FAILED on worker-1 (injected failure) 204ms
  attempt #3 FAILED on worker-1 (injected failure) 215ms
  attempt #4 FAILED on worker-1 (injected failure) 213ms

$ predisched dlq list
task_id                type          attempts  last_error
task-be875d8c          SLEEP_TASK    4         error: injected failure (failRate=1.00, draw=0.731)
```

### Timeout

```bash
predisched submit --type SLEEP_TASK --input ms=5000 --priority 5 --timeout 500 --max-retries 1
```

```
task_id=task-b227527e status=FAILED attempts=2 result='error: cancelled (timeout)'
  attempt #1 TIMED_OUT on worker-1 (TIMEOUT) 0ms
  attempt #2 TIMED_OUT on worker-1 (TIMEOUT) 0ms
```

A 5-second sleep with a 500 ms deadline is cancelled on the worker, retried once, cancelled again,
and parked.

### Ageing beats starvation

One worker with a pool of 1, `--queue-ageing 0.5`: eight priority-5 tasks queued together, then one
**priority-1** task, then a fresh priority-5 task every three seconds.

```
Next from queue: filler-6   (priority 5, waited 9304 ms, 3 still queued)
Next from queue: filler-7   (priority 5, waited 9092 ms, 2 still queued)
Next from queue: fresh-1    (priority 5, waited 7214 ms, 3 still queued)
Next from queue: starving   (priority 1, waited 13495 ms, 2 still queued)
Next from queue: fresh-2    (priority 5, waited 7792 ms, 2 still queued)
Next from queue: fresh-3    (priority 5, waited 4566 ms, 1 still queued)
Next from queue: fresh-4    (priority 5, waited 3160 ms, 0 still queued)
```

After 13.5 seconds the priority-1 task has aged to 7.75 and goes ahead of three priority-5 tasks
that arrived later. Without ageing it would sit behind every new arrival for as long as the stream
lasted.

The `Next from queue` line is logged on the queue thread on purpose: the dispatch pool finishes out
of order, so the dispatch logs cannot show what the queue chose.

## Tests

`mvn -q verify` runs 70 tests. New here:

- `AgeingPriorityQueueTest` (7): priority order, FIFO within a priority, a priority-1 task
  overtaking fresh priority-5 arrivals after 45 s of ageing, the cap, removal, snapshot order, and
  4 producers × 250 tasks with 2 consumers losing nothing. An injected clock means no sleeps.
- `RetryPolicyTest` (4): the doubling sequence and its cap, jitter within ±20 % and reproducible
  from a seed, per-task overrides, and the budget running out.
- `RetryAndTimeoutTest` (5) end to end over in-process gRPC: a flaky task succeeding on its third
  attempt with history intact; a doomed task reaching the dead-letter queue and being retried from
  it; retrying something that is not parked; a hanging task cancelled on the worker and retried;
  rejections not spending retries.

## Known limits, resolved later

- The dead-letter queue is in memory; it moves into PostgreSQL in prompt 11.
- A task whose worker dies is marked `WORKER_LOST` only when the call fails. Heartbeat-based
  detection is prompt 10.
- `CancelTask` still only works on queued tasks; cancelling a running task from the client uses the
  same machinery and arrives with the admin API (prompt 22).
