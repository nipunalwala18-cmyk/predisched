package com.predisched.scheduler.autoscale;

import com.predisched.scheduler.WorkerInfo;
import java.util.List;
import java.util.Map;

/** What the auto-scaler sees of the cluster and may do to it (prompt 21). */
public interface ClusterView {

    /**
     * @param workers healthy, non-draining workers (their load raised to the in-flight count)
     * @param schedulerQueue tasks waiting in the scheduler's own queue
     * @param inFlight tasks this scheduler has outstanding per worker
     */
    record State(List<WorkerInfo> workers, int schedulerQueue, Map<String, Integer> inFlight) {}

    State snapshot();

    /** Only the primary scales. */
    boolean isLeader();

    /** No new dispatches to this worker. */
    void drain(String workerId);

    /** The worker was stopped on purpose: drop it now instead of waiting for missed heartbeats. */
    void forget(String workerId);
}
