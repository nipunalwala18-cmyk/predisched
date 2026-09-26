package com.predisched.scheduler;

import com.predisched.proto.Heartbeat;
import com.predisched.proto.RegisterRequest;

/**
 * What the scheduler knows about one worker: its registration facts and the metrics from its last
 * heartbeat. Immutable, so a concurrent update replaces the whole record rather than mutating
 * fields another thread may be reading (rule 5).
 */
public record WorkerInfo(
        String id,
        String host,
        int port,
        int cores,
        long memoryMb,
        int poolSize,
        double cpuPct,
        double memPct,
        int activeThreads,
        int queueLen,
        long tasksCompleted,
        double avgExecMs,
        long registeredAtMs,
        long lastHeartbeatMs) {

    public static WorkerInfo fromRegistration(RegisterRequest request, long nowMs) {
        return new WorkerInfo(
                request.getWorkerId(),
                request.getHost(),
                request.getPort(),
                request.getCores(),
                request.getMemoryMb(),
                request.getPoolSize(),
                0.0, 0.0, 0, 0, 0L, 0.0,
                nowMs,
                nowMs);
    }

    public WorkerInfo withHeartbeat(Heartbeat heartbeat, long nowMs) {
        return new WorkerInfo(
                id, host, port, cores, memoryMb, poolSize,
                heartbeat.getCpuPct(),
                heartbeat.getMemPct(),
                heartbeat.getActiveThreads(),
                heartbeat.getQueueLen(),
                heartbeat.getTasksCompleted(),
                heartbeat.getAvgExecMs(),
                registeredAtMs,
                nowMs);
    }

    /** Heartbeats are due on a fixed interval; a silent worker is not a candidate for dispatch. */
    public boolean isHealthy(long nowMs, long staleAfterMs) {
        return nowMs - lastHeartbeatMs <= staleAfterMs;
    }

    /** Queued plus running work ({@code queue_len + active_threads}). */
    public int load() {
        return activeThreads + queueLen;
    }

    /**
     * This record with its load raised to at least what this scheduler has in flight there. A
     * heartbeat is up to a second old, so without this a burst would all go to whichever worker
     * looked idle at the last heartbeat; the in-flight count is exact and immediate.
     */
    public WorkerInfo withLiveLoad(int inFlight) {
        int running = Math.max(activeThreads, Math.min(inFlight, poolSize));
        int waiting = Math.max(queueLen, inFlight - poolSize);
        return new WorkerInfo(
                id, host, port, cores, memoryMb, poolSize, cpuPct, memPct,
                running, waiting, tasksCompleted, avgExecMs, registeredAtMs, lastHeartbeatMs);
    }

    public String address() {
        return host + ":" + port;
    }
}
