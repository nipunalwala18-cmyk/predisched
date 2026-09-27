package com.predisched.scheduler.strategy;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Running mean execution time per task type, and per (type, worker), from the scheduler's own
 * completed attempts (prompt 18). The prediction server gets them as {@code type_worker_mean_ms}
 * (a model feature) and {@code type_mean_ms} (the cold-start fallback).
 */
public final class ExecStats {

    private static final class Mean {
        private long count;
        private double sum;

        synchronized void add(double value) {
            count++;
            sum += value;
        }

        synchronized long count() {
            return count;
        }

        synchronized double mean() {
            return count == 0 ? 0.0 : sum / count;
        }
    }

    private final ConcurrentHashMap<String, Mean> byType = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Mean> byTypeWorker = new ConcurrentHashMap<>();

    public void record(String taskType, String workerId, long execMs) {
        byType.computeIfAbsent(taskType, k -> new Mean()).add(execMs);
        byTypeWorker.computeIfAbsent(taskType + "@" + workerId, k -> new Mean()).add(execMs);
    }

    public double mean(String taskType) {
        Mean m = byType.get(taskType);
        return m == null ? 0.0 : m.mean();
    }

    public double mean(String taskType, String workerId) {
        Mean m = byTypeWorker.get(taskType + "@" + workerId);
        return m == null ? 0.0 : m.mean();
    }

    public long count(String taskType, String workerId) {
        Mean m = byTypeWorker.get(taskType + "@" + workerId);
        return m == null ? 0 : m.count();
    }
}
