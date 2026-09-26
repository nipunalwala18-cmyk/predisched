package com.predisched.common.db;

/**
 * One finished attempt with the features captured when it was dispatched (spec §12.1): the ML
 * dataset source in prompt 15. Boxed fields are null when unknown, e.g. for an attempt a newly
 * promoted primary only collected the result of.
 */
public record ExecutionRow(
        String taskId,
        String taskType,
        String resourceProfile,
        int attempt,
        String status,
        String strategy,
        int priority,
        long inputSize,
        String workerId,
        Integer workerCores,
        Integer concurrentTasksOnWorker,
        Double cpuPct,
        Double memPct,
        Integer activeThreads,
        Integer queueLen,
        Double arrivalRate,
        Double avgExecRecent,
        long dispatchedAtMs,
        long waitTimeMs,
        long execTimeMs) {}
