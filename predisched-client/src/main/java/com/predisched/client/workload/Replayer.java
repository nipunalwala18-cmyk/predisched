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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Replays a saved trace against a scheduler with the original timing, then writes one CSV row per
 * task (submit, start, end, worker, status) and prints a per-type summary.
 *
 * <p>{@code speed} scales the clock: 2.0 replays twice as fast. A poller thread runs from the
 * first submit, so each task is timed while the rest of the trace is still being submitted:
 * {@code start_ms} is the first poll that saw the task leave QUEUED and {@code end_ms} the first
 * terminal poll, both accurate to one {@code pollMs}. A task that finished between two polls gets
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
        // Rule 5: accepted tasks reach the poller only through this queue. Every Pending is then
        // confined to the poller thread, and join() publishes its fields back to this one.
        BlockingQueue<Pending> accepted = new LinkedBlockingQueue<>();
        AtomicBoolean submitDone = new AtomicBoolean();
        AtomicReference<RuntimeException> pollFailure = new AtomicReference<>();
        List<Pending> watched = new ArrayList<>(trace.size());
        Thread poller = new Thread(() -> {
            try {
                pollUntilDone(accepted, submitDone, watched, gateway, start, pollMs, deadline);
            } catch (RuntimeException e) {
                pollFailure.set(e);
            }
        }, "replay-poller");
        poller.setDaemon(true);
        poller.start();
        try {
            for (TraceEntry entry : trace) {
                if (pollFailure.get() != null) {
                    break;
                }
                sleepUntil(start + (long) (entry.offsetMs() / speed));
                String traceId = TraceContext.newTraceId();
                TaskResponse response = gateway.submit(entry.taskId(), entry.type(),
                        entry.input(), entry.priority(), traceId, entry.timeoutMs(), -1);
                long submittedAt = System.currentTimeMillis() - start;
                if (!response.getAccepted()) {
                    results.add(new TaskResult(entry.taskId(), entry.type(), entry.priority(),
                            entry.offsetMs(), submittedAt, submittedAt, submittedAt,
                            TaskStatus.FAILED, "", 0));
                } else {
                    accepted.add(new Pending(entry, submittedAt));
                }
            }
        } catch (RuntimeException e) {
            poller.interrupt();
            throw e;
        } finally {
            submitDone.set(true);
        }
        try {
            poller.join();
        } catch (InterruptedException e) {
            poller.interrupt();
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for replayed tasks", e);
        }
        if (pollFailure.get() != null) {
            throw pollFailure.get();
        }
        // Tasks accepted after the poller hit the deadline were never polled; report them too.
        accepted.drainTo(watched);
        long now = System.currentTimeMillis() - start;
        for (Pending task : watched) {
            results.add(task.toResult(now));
        }
        results.sort((a, b) -> a.taskId().compareTo(b.taskId()));
        writeCsv(csvOut, results);
        Summary summary = new Summary();
        results.forEach(summary::add);
        summary.print(out);
        out.println("results written to " + csvOut);
        return summary;
    }

    /** An accepted task. Its mutable fields are written by the poller thread only. */
    private static final class Pending {
        final TraceEntry entry;
        final long submittedAt;
        long startMs = -1;
        long endMs = -1;
        TaskStatus status = TaskStatus.QUEUED;
        String worker = "";
        long execMs;

        Pending(TraceEntry entry, long submittedAt) {
            this.entry = entry;
            this.submittedAt = submittedAt;
        }

        /** A task the poller never saw finish is reported FAILED at {@code now}. */
        TaskResult toResult(long now) {
            boolean finished = endMs >= 0;
            long end = finished ? endMs : now;
            return new TaskResult(entry.taskId(), entry.type(), entry.priority(),
                    entry.offsetMs(), submittedAt, startMs >= 0 ? startMs : end, end,
                    finished ? status : TaskStatus.FAILED, worker, execMs);
        }
    }

    /**
     * Polls every accepted, unfinished task each {@code pollMs} until the submit loop is done and
     * every task is terminal, the deadline passes, or the thread is interrupted.
     */
    private static void pollUntilDone(
            BlockingQueue<Pending> accepted,
            AtomicBoolean submitDone,
            List<Pending> watched,
            SchedulerGateway gateway,
            long start,
            long pollMs,
            long deadline) {
        while (true) {
            // Read the flag before draining: once it is true, every task is already in the queue.
            boolean lastSweep = submitDone.get();
            accepted.drainTo(watched);
            boolean unfinished = false;
            for (Pending task : watched) {
                if (task.endMs >= 0) {
                    continue;
                }
                TaskStatusResponse response = gateway.status(task.entry.taskId());
                long now = System.currentTimeMillis() - start;
                TaskStatus status = response.getStatus();
                task.worker = response.getWorkerId();
                task.execMs = response.getExecTimeMs();
                if (task.startMs < 0 && status != TaskStatus.QUEUED) {
                    task.startMs = now;
                }
                if (status == TaskStatus.COMPLETED
                        || status == TaskStatus.FAILED
                        || status == TaskStatus.CANCELLED) {
                    task.status = status;
                    task.endMs = now;
                } else {
                    unfinished = true;
                }
            }
            if ((lastSweep && !unfinished) || System.currentTimeMillis() > deadline) {
                return;
            }
            try {
                Thread.sleep(pollMs);
            } catch (InterruptedException e) {
                return;
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
