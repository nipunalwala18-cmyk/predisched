package com.predisched.scheduler.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * What is running right now, so the {@link com.predisched.scheduler.TimeoutWatcher} can find tasks
 * that have outstayed their deadline (FR26) and the worker failure detector can find the tasks a
 * dead worker was running (prompt 10).
 *
 * <p>The watcher does not decide the task's fate: it cancels the execution on the worker and marks
 * the attempt timed out. The dispatcher's own call then returns and handles the outcome, so exactly
 * one place records the attempt however it ended.
 *
 * <p>An attempt is <em>settled</em> exactly once, by whoever gets there first: the dispatcher when
 * its call returns, or the failure detector when the worker is declared dead. The loser leaves the
 * task alone, so a late reply from a worker given up on cannot overwrite the retry.
 */
public class RunningTasks {

    /** A task in flight. {@code deadlineMs} of 0 means no timeout applies. */
    public record Running(
            String taskId,
            String workerId,
            int attempt,
            long startedAtMs,
            long deadlineMs,
            AtomicBoolean timedOut,
            AtomicBoolean settled) {

        public boolean isOverdue(long nowMs) {
            return deadlineMs > 0 && nowMs > deadlineMs && !timedOut.get();
        }
    }

    private final ConcurrentHashMap<String, Running> running = new ConcurrentHashMap<>();

    public Running start(
            String taskId, String workerId, int attempt, long startedAtMs, long deadlineMs) {
        Running entry = new Running(taskId, workerId, attempt, startedAtMs, deadlineMs,
                new AtomicBoolean(), new AtomicBoolean());
        running.put(taskId, entry);
        return entry;
    }

    /**
     * Settles an attempt and forgets it. False if it was already settled elsewhere, in which case
     * the caller must not record an outcome for it.
     */
    public boolean finish(Running entry) {
        boolean mine = entry.settled().compareAndSet(false, true);
        running.remove(entry.taskId(), entry);
        return mine;
    }

    /** Settles and removes every attempt on one worker; returns the ones settled here. */
    public List<Running> settleAllOn(String workerId) {
        List<Running> settled = new ArrayList<>();
        for (Running entry : running.values()) {
            if (entry.workerId().equals(workerId) && finish(entry)) {
                settled.add(entry);
            }
        }
        return settled;
    }

    /** Forgets everything, when a new primary rebuilds its state from the store. */
    public void clear() {
        running.clear();
    }

    public Optional<Running> get(String taskId) {
        return Optional.ofNullable(running.get(taskId));
    }

    /** Tasks past their deadline that have not been marked yet. */
    public List<Running> overdue(long nowMs) {
        List<Running> due = new ArrayList<>();
        for (Running entry : running.values()) {
            if (entry.isOverdue(nowMs)) {
                due.add(entry);
            }
        }
        return due;
    }

    public int size() {
        return running.size();
    }

    /** How many tasks this scheduler currently has outstanding on one worker. */
    public int countFor(String workerId) {
        int count = 0;
        for (Running entry : running.values()) {
            if (entry.workerId().equals(workerId)) {
                count++;
            }
        }
        return count;
    }
}
