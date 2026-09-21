package com.predisched.scheduler.queue;

import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.time.Clocks;
import com.predisched.proto.TaskStatus;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides what happens after an attempt ends (F4, FR25): succeed, retry after a backoff, or park in
 * the dead-letter queue.
 *
 * <p>Keeping this out of the dispatcher means the dispatch path stays "pick a worker, call it,
 * report what happened", and the retry rules live in one place with the policy and the DLQ.
 */
public class RetryCoordinator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RetryCoordinator.class);

    /** Outcomes that consume a retry. A rejection does not: the worker never ran the task. */
    private static boolean countsAgainstRetries(TaskAttempt.Outcome outcome) {
        return outcome == TaskAttempt.Outcome.FAILED
                || outcome == TaskAttempt.Outcome.TIMED_OUT
                || outcome == TaskAttempt.Outcome.WORKER_LOST;
    }

    private final TaskStore store;
    private final TaskQueue queue;
    private final RetryPolicy policy;
    private final DeadLetterQueue deadLetters;
    private final ScheduledExecutorService scheduler;

    public RetryCoordinator(
            TaskStore store, TaskQueue queue, RetryPolicy policy, DeadLetterQueue deadLetters) {
        this.store = store;
        this.queue = queue;
        this.policy = policy;
        this.deadLetters = deadLetters;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "retry-scheduler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Records a successful attempt and completes the task. */
    public void succeeded(String taskId, TaskAttempt attempt, String output, long execMs) {
        store.update(taskId, record -> record
                .withAttempt(attempt)
                .withResult(output)
                .withExecTimeMs(execMs)
                .withStatus(TaskStatus.COMPLETED));
    }

    /**
     * Records a failed attempt and either schedules a retry or parks the task.
     *
     * @return true when the task will be tried again
     */
    public boolean failed(String taskId, TaskAttempt attempt, String output) {
        TaskRecord updated = store.update(taskId, record -> record.withAttempt(attempt));
        long failures = updated.attempts().stream()
                .filter(a -> countsAgainstRetries(a.outcome()))
                .count();
        int maxRetries = policy.maxRetriesFor(updated.maxRetries());

        if (!countsAgainstRetries(attempt.outcome())) {
            // Rejected by a full worker queue: the worker never ran it, so this does not spend a
            // retry. Put it straight back and let the next dispatch pick another worker.
            requeue(taskId, 0);
            return true;
        }
        if (failures <= maxRetries) {
            long delayMs = policy.delayMsAfter((int) failures);
            log.info("Attempt {} of {} failed ({}); retrying in {} ms ({} of {} retries used)",
                    attempt.attempt(), taskId, attempt.reason(), delayMs, failures, maxRetries);
            EventLog.get().event("RETRY_SCHEDULED", taskId, Map.of(
                    "attempt", String.valueOf(attempt.attempt()),
                    "reason", attempt.reason(),
                    "delay_ms", String.valueOf(delayMs)));
            requeue(taskId, delayMs);
            return true;
        }

        // Out of retries. Park it first, then make it FAILED: a client that reacts to the status
        // would otherwise be able to list the dead letters before the task had arrived there.
        TaskRecord parked = updated.withResult(output).withStatus(TaskStatus.FAILED);
        deadLetters.add(parked, attempt.reason(), Clocks.now());
        store.update(taskId, record -> record
                .withResult(output)
                .withStatus(TaskStatus.FAILED));
        log.warn("Task {} exhausted {} retries after {} attempts; parked in the dead-letter queue",
                taskId, maxRetries, parked.attemptCount());
        EventLog.get().event("DEAD_LETTER", taskId, Map.of(
                "attempts", String.valueOf(parked.attemptCount()),
                "reason", attempt.reason()));
        return false;
    }

    /**
     * Puts a parked task back in the queue (F4). The task is FAILED, and FAILED is terminal, so the
     * record is replaced with a fresh QUEUED one that keeps the original request and its history.
     */
    public boolean retryDeadLetter(String taskId) {
        return deadLetters.remove(taskId).map(entry -> {
            TaskRecord parked = entry.record();
            store.replace(TaskRecord.createQueued(
                    parked.id(), parked.type(), parked.input(), parked.priority(),
                    parked.traceId(), parked.timeoutMs(), parked.maxRetries()));
            queue.add(parked.id(), parked.priority(), Clocks.now());
            log.info("Task {} taken out of the dead-letter queue and re-queued", taskId);
            return true;
        }).orElse(false);
    }

    /**
     * Moves the task back to QUEUED now (so a client polling in the meantime sees the truth) and
     * puts it in the queue after the backoff.
     */
    private void requeue(String taskId, long delayMs) {
        store.update(taskId, record ->
                record.status() == TaskStatus.QUEUED
                        ? record
                        : record.withStatus(TaskStatus.QUEUED));
        Runnable put = () -> {
            LamportInterceptors.applyMdc();
            TaskRecord record = store.get(taskId);
            if (record == null || record.status() != TaskStatus.QUEUED) {
                return;
            }
            queue.add(taskId, record.priority(), Clocks.now());
        };
        if (delayMs <= 0) {
            put.run();
        } else {
            scheduler.schedule(put, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    public DeadLetterQueue deadLetters() {
        return deadLetters;
    }

    public RetryPolicy policy() {
        return policy;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
