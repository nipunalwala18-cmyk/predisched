package com.predisched.scheduler.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * What is running right now, so the {@link com.predisched.scheduler.TimeoutWatcher} can find tasks
 * that have outstayed their deadline (FR26).
 *
 * <p>The watcher does not decide the task's fate: it cancels the execution on the worker and marks
 * the attempt timed out. The dispatcher's own call then returns and handles the outcome, so exactly
 * one place records the attempt however it ended.
 */
public class RunningTasks {

    /** A task in flight. {@code deadlineMs} of 0 means no timeout applies. */
    public record Running(
            String taskId,
            String workerId,
            int attempt,
            long startedAtMs,
            long deadlineMs,
            AtomicBoolean timedOut) {

        public boolean isOverdue(long nowMs) {
            return deadlineMs > 0 && nowMs > deadlineMs && !timedOut.get();
        }
    }

    private final ConcurrentHashMap<String, Running> running = new ConcurrentHashMap<>();

    public Running start(
            String taskId, String workerId, int attempt, long startedAtMs, long deadlineMs) {
        Running entry =
                new Running(taskId, workerId, attempt, startedAtMs, deadlineMs, new AtomicBoolean());
        running.put(taskId, entry);
        return entry;
    }

    public Optional<Running> finish(String taskId) {
        return Optional.ofNullable(running.remove(taskId));
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
