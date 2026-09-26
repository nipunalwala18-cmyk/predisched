package com.predisched.common.db;

import com.predisched.common.TaskRecord;
import java.util.Map;

/**
 * Where a node reports what should end up in the database (spec §14). Every method returns at
 * once: {@link HistoryWriter} queues the row and writes it in a batch later, so the scheduler never
 * waits on the database. The default methods do nothing, which is what a node without a
 * database gets ({@link History#get()}).
 */
public interface HistorySink {

    HistorySink NONE = new HistorySink() {};

    /** The current state of a task (upsert into {@code tasks}). */
    default void task(TaskRecord record) {}

    default void worker(String workerId, String host, int port, int cores, long memoryMb,
            int poolSize, String status, long lastHeartbeatMs) {}

    default void workerMetrics(String workerId, long tsMs, double cpuPct, double memPct,
            int activeThreads, int queueLen, long tasksCompleted, double avgExecMs) {}

    default void decision(String taskId, String strategy, String chosenWorker, Double cost,
            Map<String, Double> scores, long decisionUs, long tsMs) {}

    default void execution(ExecutionRow row) {}

    default void event(String nodeId, long lamportTime, long tsMs, String type, String taskId,
            String traceId, Map<String, String> details) {}

    default void replication(long seqNo, String nodeId, String op, String taskId, long version,
            long lamportTime, long appliedAtMs) {}

    default void clockSync(String nodeId, long tsMs, long offsetBeforeMs, long offsetAfterMs,
            String algorithm) {}

    default void failure(String nodeId, String type, long detectedAtMs, String details) {}

    /** Closes the node's open failures. */
    default void recovered(String nodeId, long recoveredAtMs) {}

    default void cacheStore(String key, String taskType, String output) {}

    default void cacheHit(String key) {}
}
