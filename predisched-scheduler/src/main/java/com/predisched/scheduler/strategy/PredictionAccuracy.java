package com.predisched.scheduler.strategy;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Live accuracy of the execution-time predictions (prompt 18): the absolute error of each
 * completed task's prediction for the worker it ran on, as a rolling MAE over the last
 * {@code window} tasks, overall and per task type. Guarded by this.
 */
public final class PredictionAccuracy {

    /** The state after one more completed task. */
    public record Snapshot(long tasks, double mae, int window, double typeMae, int typeWindow) {}

    private static final class Rolling {
        private final Deque<Double> errors = new ArrayDeque<>();
        private final int capacity;
        private double sum;

        Rolling(int capacity) {
            this.capacity = capacity;
        }

        void add(double error) {
            errors.addLast(error);
            sum += error;
            if (errors.size() > capacity) {
                sum -= errors.removeFirst();
            }
        }

        double mae() {
            return errors.isEmpty() ? Double.NaN : sum / errors.size();
        }

        int size() {
            return errors.size();
        }
    }

    private final int window;
    private final Rolling overall;
    private final Map<String, Rolling> byType = new LinkedHashMap<>();
    private long tasks;

    public PredictionAccuracy(int window) {
        this.window = window;
        this.overall = new Rolling(window);
    }

    public synchronized Snapshot record(String taskType, double predictedMs, double actualMs) {
        double error = Math.abs(predictedMs - actualMs);
        overall.add(error);
        Rolling type = byType.computeIfAbsent(taskType, k -> new Rolling(window));
        type.add(error);
        tasks++;
        return new Snapshot(tasks, overall.mae(), overall.size(), type.mae(), type.size());
    }

    public synchronized double mae() {
        return overall.mae();
    }

    public synchronized long tasks() {
        return tasks;
    }

    /** Rolling MAE per task type, in first-seen order. */
    public synchronized Map<String, Double> maeByType() {
        Map<String, Double> out = new LinkedHashMap<>();
        byType.forEach((type, rolling) -> out.put(type, rolling.mae()));
        return out;
    }
}
