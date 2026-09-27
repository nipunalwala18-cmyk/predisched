package com.predisched.scheduler;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The most recent completed execution times per task type, for the straggler threshold's p90
 * (prompt 19, F11).
 */
public final class TypeExecTimes {

    private final int window;
    private final Map<String, Deque<Long>> byType = new ConcurrentHashMap<>();

    public TypeExecTimes(int window) {
        this.window = window;
    }

    public void record(String taskType, long execMs) {
        Deque<Long> times = byType.computeIfAbsent(taskType, k -> new ArrayDeque<>());
        synchronized (times) {
            times.addLast(execMs);
            if (times.size() > window) {
                times.removeFirst();
            }
        }
    }

    /** Nearest-rank p90 once there are {@code minSamples} runs of the type, else empty. */
    public OptionalDouble p90(String taskType, int minSamples) {
        Deque<Long> times = byType.get(taskType);
        if (times == null) {
            return OptionalDouble.empty();
        }
        long[] sorted;
        synchronized (times) {
            if (times.size() < minSamples) {
                return OptionalDouble.empty();
            }
            sorted = times.stream().mapToLong(Long::longValue).toArray();
        }
        Arrays.sort(sorted);
        int rank = (int) Math.ceil(0.9 * sorted.length) - 1;
        return OptionalDouble.of(sorted[Math.max(0, rank)]);
    }
}
