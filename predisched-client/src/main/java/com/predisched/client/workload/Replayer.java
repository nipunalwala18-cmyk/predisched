package com.predisched.client.workload;

import com.predisched.common.obs.TraceContext;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Replays a saved trace against a scheduler with the original timing, then writes one CSV row per
 * task (submit, start, end, worker, status) and prints a per-type summary.
 *
 * <p>{@code speed} scales the clock: 2.0 replays twice as fast. {@code start_ms} is the first
 * poll that saw the task leave QUEUED; a task that finished between polls gets
 * {@code start_ms == end_ms}.
 */
public final class Replayer {

    /** One replayed task, in the order it was submitted. */
    public record TaskResult(
            String taskId,
            TaskType type,
            int priority,
            long offsetMs,
            long submitMs,
            long startMs,
            long endMs,
            TaskStatus status,
            String worker,
            long execMs) {}

    /** Per-type outcome counts and mean execution times. */
    public static final class Summary {
        private final Map<TaskType, long[]> counts = new EnumMap<>(TaskType.class);
        private final Map<TaskType, long[]> execSums = new EnumMap<>(TaskType.class);

        void add(TaskResult result) {
            counts.computeIfAbsent(result.type(), t -> new long[2]);
            execSums.computeIfAbsent(result.type(), t -> new long[2]);
            if (result.status() == TaskStatus.COMPLETED) {
                counts.get(result.type())[0]++;
                execSums.get(result.type())[0] += result.execMs();
                execSums.get(result.type())[1]++;
            } else {
                counts.get(result.type())[1]++;
            }
        }

        /** Completed count for a type. */
        public long completed(TaskType type) {
            return counts.getOrDefault(type, new long[2])[0];
        }

        /** Non-completed count for a type. */
        public long failed(TaskType type) {
            return counts.getOrDefault(type, new long[2])[1];
        }

        /** Mean worker execution time over completed tasks of a type, or -1 when none. */
        public double meanExecMs(TaskType type) {
            long[] sums = execSums.getOrDefault(type, new long[2]);
            return sums[1] == 0 ? -1 : (double) sums[0] / sums[1];
        }

        public long totalCompleted() {
            return counts.values().stream().mapToLong(c -> c[0]).sum();
        }

        public long totalFailed() {
            return counts.values().stream().mapToLong(c -> c[1]).sum();
        }

        void print(PrintStream out) {
            out.println("replay summary:");
            for (Map.Entry<TaskType, long[]> entry : counts.entrySet()) {
                out.printf(Locale.ROOT, "  %s: completed=%d failed=%d mean_exec_ms=%.1f%n",
                        entry.getKey(), entry.getValue()[0], entry.getValue()[1],
                        meanExecMs(entry.getKey()));
            }
            out.printf(Locale.ROOT, "overall: completed=%d failed=%d%n",
                    totalCompleted(), totalFailed());
        }
    }

    private Replayer() {}

    public static Summary replay(
            List<TraceEntry> trace,
            double speed,
            SchedulerGateway gateway,
            Path csvOut,
            long pollMs,
            long overallTimeoutMs,
            PrintStream out) throws IOException {
        if (trace.isEmpty()) {
            throw new IllegalArgumentException("trace is empty");
        }
        if (speed <= 0) {
            throw new IllegalArgumentException("speed must be positive");
        }
        List<TaskResult> results = new ArrayList<>(trace.size());
        long start = System.currentTimeMillis();
        long deadline = start + overallTimeoutMs;
        List<Pending> pending = new ArrayList<>(trace.size());
        for (TraceEntry entry : trace) {
            long dueAt = start + (long) (entry.offsetMs() / speed);
            sleepUntil(dueAt);
            String traceId = TraceContext.newTraceId();
            TaskResponse response = gateway.submit(entry.taskId(), entry.type(), entry.input(),
                    entry.priority(), traceId, entry.timeoutMs(), -1);
            long submittedAt = System.currentTimeMillis() - start;
            if (!response.getAccepted()) {
                results.add(new TaskResult(entry.taskId(), entry.type(), entry.priority(),
                        entry.offsetMs(), submittedAt, submittedAt, submittedAt,
                        TaskStatus.FAILED, "", 0));
            } else {
                pending.add(new Pending(entry, submittedAt));
            }
        }
        for (Pending task : pending) {
            results.add(awaitTerminal(task, start, gateway, pollMs, deadline));
        }
        results.sort((a, b) -> a.taskId().compareTo(b.taskId()));
        writeCsv(csvOut, results);
        Summary summary = new Summary();
        results.forEach(summary::add);
        summary.print(out);
        out.println("results written to " + csvOut);
        return summary;
    }

    private record Pending(TraceEntry entry, long submittedAt) {}

    private static TaskResult awaitTerminal(
            Pending task, long start, SchedulerGateway gateway, long pollMs, long deadline) {
        long firstActive = -1;
        while (true) {
            TaskStatusResponse response = gateway.status(task.entry().taskId());
            TaskStatus status = response.getStatus();
            long now = System.currentTimeMillis() - start;
            if (firstActive == -1 && status != TaskStatus.QUEUED) {
                firstActive = now;
            }
            if (status == TaskStatus.COMPLETED
                    || status == TaskStatus.FAILED
                    || status == TaskStatus.CANCELLED) {
                return new TaskResult(
                        task.entry().taskId(), task.entry().type(), task.entry().priority(),
                        task.entry().offsetMs(), task.submittedAt(),
                        firstActive == -1 ? now : firstActive, now, status,
                        response.getWorkerId(), response.getExecTimeMs());
            }
            if (System.currentTimeMillis() > deadline) {
                return new TaskResult(
                        task.entry().taskId(), task.entry().type(), task.entry().priority(),
                        task.entry().offsetMs(), task.submittedAt(),
                        firstActive == -1 ? now : firstActive, now, TaskStatus.FAILED,
                        response.getWorkerId(), response.getExecTimeMs());
            }
            try {
                Thread.sleep(pollMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                long at = System.currentTimeMillis() - start;
                return new TaskResult(
                        task.entry().taskId(), task.entry().type(), task.entry().priority(),
                        task.entry().offsetMs(), task.submittedAt(),
                        firstActive == -1 ? at : firstActive, at, TaskStatus.FAILED, "", 0);
            }
        }
    }

    private static void sleepUntil(long dueAt) {
        while (true) {
            long wait = dueAt - System.currentTimeMillis();
            if (wait <= 0) {
                return;
            }
            try {
                Thread.sleep(Math.min(wait, 50));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    static void writeCsv(Path path, List<TaskResult> results) throws IOException {
        StringBuilder out = new StringBuilder();
        out.append("task_id,task_type,priority,offset_ms,submit_ms,start_ms,end_ms,"
                + "latency_ms,status,worker,exec_ms\n");
        for (TaskResult result : results) {
            out.append(csv(result.taskId())).append(',')
                    .append(result.type()).append(',')
                    .append(result.priority()).append(',')
                    .append(result.offsetMs()).append(',')
                    .append(result.submitMs()).append(',')
                    .append(result.startMs()).append(',')
                    .append(result.endMs()).append(',')
                    .append(result.endMs() - result.submitMs()).append(',')
                    .append(result.status()).append(',')
                    .append(csv(result.worker())).append(',')
                    .append(result.execMs()).append('\n');
        }
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Files.writeString(path, out.toString(), StandardCharsets.UTF_8);
    }

    private static String csv(String raw) {
        if (raw.contains(",") || raw.contains("\"") || raw.contains("\n")) {
            return "\"" + raw.replace("\"", "\"\"") + "\"";
        }
        return raw;
    }
}
