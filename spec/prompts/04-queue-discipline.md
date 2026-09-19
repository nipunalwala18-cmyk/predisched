# Prompt 04: Priority ageing, retries with backoff, dead-letter queue, timeouts (F2, F4, F5)

## Goal

The scheduler queue orders tasks by priority without starving low-priority work, failed tasks retry
with exponential backoff and then land in a dead-letter queue, and runaway tasks are timed out and
cancelled on the worker.

## Read first

- Spec §4 FR23, FR25, FR26, §6 Tier 1 (F2, F4, F5), §3.3

## Build

1. **Protos**: add to `TaskRequest` (next free numbers) `int64 timeout_ms`, `int32 max_retries`.
   Add `int32 attempt` to `TaskStatusResponse`. Add `WorkerService.CancelExecution(TaskStatusRequest)
   returns (Ack)`. Add `SchedulerService.ListDeadLetters` and `RetryDeadLetter`.
2. **`predisched-scheduler`**
   - `TaskQueue` interface; `AgeingPriorityQueue` implementation: effective priority =
     `priority + ageing.rate × seconds_waiting`, capped at 10. Ties by submit Lamport time.
     Recompute lazily on `take()` so the queue does not need a timer. Thread-safe with one
     documented lock and a `Condition`.
   - `RetryPolicy`: exponential backoff `base × 2^attempt` with jitter from the seeded `Random`,
     capped at `retry.max.delay.ms`. Default `max_retries` from config when the request omits it.
   - Failed attempts go back into the queue after their backoff via a `ScheduledExecutorService`.
     When retries are exhausted the task moves to `FAILED` and into `DeadLetterQueue`.
   - `TimeoutWatcher`: tracks running tasks; on expiry calls `CancelExecution` on the worker, marks
     the attempt failed with reason `TIMEOUT`, and lets `RetryPolicy` decide what happens next
     (re-dispatch elsewhere is prompt 08's job once strategies exist).
   - Record attempt history on `TaskRecord` (attempt number, worker, outcome, reason).
3. **`predisched-worker`**
   - Executors check `Thread.interrupted()` in their loops; `CancelExecution` cancels the future
     with interrupt. `SleepTaskExecutor` sleeps interruptibly.
   - A test-only `failRate` input key (e.g. `ms=200, failRate=0.5, seed=7`) on `SLEEP_TASK` makes
     failures reproducible for demos and tests.
4. **`predisched-client`**: `dlq list`, `dlq retry <id>`; `status` shows the attempt history.

## Tests

- `AgeingPriorityQueueTest`: higher priority first; a priority-1 task waiting long enough overtakes
  newly arrived priority-5 tasks (use an injected clock, no sleeps).
- `RetryPolicyTest`: delays for attempts 0..5 with a fixed seed; cap respected.
- `TimeoutTest`: a `SLEEP_TASK ms=5000` with `timeout_ms=500` ends as a timed-out attempt and the
  worker thread is freed.
- `DeadLetterTest`: a task that always fails ends in the DLQ after `max_retries + 1` attempts, and
  `RetryDeadLetter` puts it back in the queue.

## Acceptance checks

```bash
java -jar predisched-client/target/predisched-client.jar submit --type SLEEP_TASK --input "ms=200, failRate=1.0, seed=1" --priority 5
```
```bash
java -jar predisched-client/target/predisched-client.jar dlq list
```
```bash
java -jar predisched-client/target/predisched-client.jar submit --type SLEEP_TASK --input ms=5000 --priority 5 --timeout 500
```

Paste the attempt history with backoff delays, the DLQ listing, and the timeout outcome.

## Docs and commit

- `docs/components/queue.md`: ageing formula, retry policy, DLQ, timeouts, with real output.
- Commit: `Queue: priority ageing, retries with backoff, dead-letter queue and task timeouts`
