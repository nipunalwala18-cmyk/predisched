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

    /** Queued plus running work, the simple load signal until prompt 08 scores workers properly. */
    public int load() {
        return activeThreads + queueLen;
    }

    public String address() {
        return host + ":" + port;
    }
}
