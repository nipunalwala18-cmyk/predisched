package com.predisched.fault;

/**
 * The scheduler's side of a promotion or demotion. The coordinator decides when; the scheduler
 * owns the queue and the dispatcher, so it does the work. The fault module never imports
 * scheduler classes (spec §9): the scheduler implements this.
 */
public interface PrimaryRole {

    /**
     * Replaces this node's in-memory scheduling state (queue, in-flight map, workflow edges,
     * dead letters, quotas) with what the replicated store says. RUNNING tasks are not queued:
     * the coordinator hands them to the {@link InDoubtResolver} next.
     */
    void rebuild(Recovery recovery);

    void startDispatching();

    /** Stops taking tasks off the queue; calls already in flight finish on their own. */
    void stopDispatching();
}
