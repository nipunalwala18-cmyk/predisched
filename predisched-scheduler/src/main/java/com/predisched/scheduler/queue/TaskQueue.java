package com.predisched.scheduler.queue;

import java.util.List;

/**
 * The scheduler's pending work. The dispatcher only knows this interface, so the ordering policy
 * can change (ageing priority today, per-user fair share in spec §6 F19) without touching dispatch.
 */
public interface TaskQueue {

    /** Adds a task id. Re-queuing after a failed attempt uses this too. */
    void add(String taskId, int priority, long queuedAtMs);

    /** Blocks until a task is available and returns the one that should run next. */
    String take() throws InterruptedException;

    /** Removes a task that no longer needs to run (cancelled). True if it was queued. */
    boolean remove(String taskId);

    int size();

    /** Queued ids in the order they would be taken, for logs and tests. */
    List<String> snapshot();
}
