package com.predisched.benchmark.suite;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The spec §13 metrics of one run, from the replay CSV (latency, throughput, makespan, imbalance,
 * queue, SLA) plus what only the database knows (CPU, scheduling overhead, prediction error).
 */
public final class RunMetrics {

    /** One task of a replay CSV (Replayer's columns). */
    public record TaskRow(String taskId, String taskType, long offsetMs, long submitMs,
            long startMs, long endMs, long latencyMs, String status, String worker, long execMs) {

        boolean completed() {
            return "COMPLETED".equals(status);
        }
    }

    /** Metrics in a fixed order: the runs.csv columns after the run's identity. */
    public static final List<String> NAMES = List.of(
            "tasks", "completed", "failed", "mean_latency_ms", "p95_latency_ms",
            "p99_latency_ms", "throughput_per_s", "makespan_ms", "cpu_mean_pct", "cpu_var",
            "imbalance", "max_queue", "sla_violation_pct", "overhead_mean_us",
            "overhead_p95_us", "mae_ms", "scale_ups", "scale_downs");

    private RunMetrics() {}

    public static List<TaskRow> read(Path csv) throws IOException {
        List<String> lines = Files.readAllLines(csv, StandardCharsets.UTF_8);
        List<TaskRow> rows = new ArrayList<>();
        for (String line : lines.subList(1, lines.size())) {
            if (line.isBlank()) {
                continue;
            }
            // task_id,task_type,priority,offset_ms,submit_ms,start_ms,end_ms,latency_ms,status,worker,exec_ms
            String[] f = line.split(",", -1);
            rows.add(new TaskRow(f[0], f[1], Long.parseLong(f[3]), Long.parseLong(f[4]),
                    parseOr(f[5], -1), parseOr(f[6], -1), parseOr(f[7], -1), f[8], f[9],
                    parseOr(f[10], 0)));
        }
        return rows;
    }

    private static long parseOr(String text, long fallback) {
        try {
            return text.isBlank() ? fallback : Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /** Nearest-rank percentile of sorted values; NaN when empty. */
    public static double percentile(double[] sorted, double p) {
        if (sorted.length == 0) {
            return Double.NaN;
        }
        int rank = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, rank))];
    }

    /** Population standard deviation of completed tasks per worker, idle workers included. */
    public static double imbalance(List<TaskRow> rows, List<String> workers) {
        Map<String, Integer> perWorker = new LinkedHashMap<>();
        workers.forEach(w -> perWorker.put(w, 0));
        for (TaskRow r : rows) {
            if (r.completed() && perWorker.containsKey(r.worker())) {
                perWorker.merge(r.worker(), 1, Integer::sum);
            }
        }
        double mean = perWorker.values().stream().mapToInt(Integer::intValue).average().orElse(0);
        return Math.sqrt(perWorker.values().stream()
                .mapToDouble(n -> (n - mean) * (n - mean)).average().orElse(0));
    }

    /**
     * The most tasks waiting at once: submitted and not yet started (a sweep over the
     * [submit, start) intervals). A task never started waits until its end, or forever.
     */
    public static int maxQueue(List<TaskRow> rows) {
        List<long[]> events = new ArrayList<>();
        for (TaskRow r : rows) {
            long until = r.startMs() >= 0 ? r.startMs() : r.endMs() >= 0 ? r.endMs() : Long.MAX_VALUE;
            if (until > r.submitMs()) {
                events.add(new long[] {r.submitMs(), +1});
                events.add(new long[] {until, -1});
            }
        }
        // At equal times a start (-1) comes before a submit (+1).
        events.sort(Comparator.<long[]>comparingLong(e -> e[0]).thenComparingLong(e -> e[1]));
        int current = 0;
        int max = 0;
        for (long[] e : events) {
            current += (int) e[1];
            max = Math.max(max, current);
        }
        return max;
    }

    /** % of tasks that failed or finished later than {@code deadlineMs} after their submit. */
    public static double slaViolationPct(List<TaskRow> rows, long deadlineMs) {
        if (rows.isEmpty()) {
            return Double.NaN;
        }
        long violations = rows.stream()
                .filter(r -> !r.completed() || r.latencyMs() > deadlineMs).count();
        return 100.0 * violations / rows.size();
    }

    /**
     * Everything the CSV gives. The first {@code warmup} tasks by trace offset are left out of the
     * latency percentiles; throughput, makespan, imbalance, queue and SLA use every task.
     */
    public static Map<String, Double> fromReplay(List<TaskRow> rows, List<String> workers,
            int warmup, long deadlineMs) {
        List<TaskRow> byOffset = new ArrayList<>(rows);
        byOffset.sort(Comparator.comparingLong(TaskRow::offsetMs)
                .thenComparing(TaskRow::taskId));
        double[] latencies = byOffset.subList(Math.min(warmup, byOffset.size()), byOffset.size())
                .stream().filter(TaskRow::completed).mapToDouble(TaskRow::latencyMs).sorted()
                .toArray();
        long completed = rows.stream().filter(TaskRow::completed).count();
        long firstSubmit = rows.stream().mapToLong(TaskRow::submitMs).min().orElse(0);
        long lastEnd = rows.stream().mapToLong(TaskRow::endMs).max().orElse(firstSubmit);
        long makespan = Math.max(1, lastEnd - firstSubmit);
        Map<String, Double> m = new LinkedHashMap<>();
        m.put("tasks", (double) rows.size());
        m.put("completed", (double) completed);
        m.put("failed", (double) (rows.size() - completed));
        m.put("mean_latency_ms", Arrays.stream(latencies).average().orElse(Double.NaN));
        m.put("p95_latency_ms", percentile(latencies, 95));
        m.put("p99_latency_ms", percentile(latencies, 99));
        m.put("throughput_per_s", completed * 1000.0 / makespan);
        m.put("makespan_ms", (double) makespan);
        m.put("imbalance", imbalance(rows, workers));
        m.put("max_queue", (double) maxQueue(rows));
        m.put("sla_violation_pct", slaViolationPct(rows, deadlineMs));
        return m;
    }
}
