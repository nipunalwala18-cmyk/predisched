package com.predisched.worker;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Live worker metrics for heartbeats (FR7, FR13).
 *
 * <p>All counters are atomic and the rolling window is guarded by its own lock, so the executing
 * threads, the gRPC threads and the heartbeat thread can touch this object at once (rule 5).
 */
public class WorkerMetrics {

    /** How many recent tasks the rolling average covers. */
    static final int WINDOW = 50;

    private final AtomicLong tasksCompleted = new AtomicLong();
    private final AtomicLong tasksFailed = new AtomicLong();
    private final AtomicLong tasksRejected = new AtomicLong();

    private final Object windowLock = new Object();
    private final long[] recentExecMs = new long[WINDOW];
    private int windowCount;
    private int windowNext;

    private final java.lang.management.OperatingSystemMXBean os =
            ManagementFactory.getOperatingSystemMXBean();

    public void recordCompleted(long execMs) {
        tasksCompleted.incrementAndGet();
        synchronized (windowLock) {
            recentExecMs[windowNext] = execMs;
            windowNext = (windowNext + 1) % WINDOW;
            if (windowCount < WINDOW) {
                windowCount++;
            }
        }
    }

    public void recordFailed(long execMs) {
        tasksFailed.incrementAndGet();
        recordCompleted(execMs);
    }

    public void recordRejected() {
        tasksRejected.incrementAndGet();
    }

    public long tasksCompleted() {
        return tasksCompleted.get();
    }

    public long tasksFailed() {
        return tasksFailed.get();
    }

    public long tasksRejected() {
        return tasksRejected.get();
    }

    /** Mean execution time over the last {@link #WINDOW} tasks, or 0 before any have run. */
    public double avgExecMs() {
        synchronized (windowLock) {
            if (windowCount == 0) {
                return 0.0;
            }
            long sum = 0;
            for (int i = 0; i < windowCount; i++) {
                sum += recentExecMs[i];
            }
            return (double) sum / windowCount;
        }
    }

    /**
     * Process CPU load as a percentage. Uses the JDK-internal bean when it is available and falls
     * back to system load average per core, which is all some platforms expose.
     */
    public double cpuPct() {
        if (os instanceof com.sun.management.OperatingSystemMXBean sun) {
            double load = sun.getProcessCpuLoad();
            if (load >= 0) {
                return load * 100.0;
            }
        }
        double load = os.getSystemLoadAverage();
        int cores = Math.max(1, os.getAvailableProcessors());
        return load < 0 ? 0.0 : Math.min(100.0, load / cores * 100.0);
    }

    /** Heap in use as a percentage of the maximum heap. */
    public double memPct() {
        Runtime runtime = Runtime.getRuntime();
        long max = runtime.maxMemory();
        if (max <= 0) {
            return 0.0;
        }
        long used = runtime.totalMemory() - runtime.freeMemory();
        return (double) used / max * 100.0;
    }

    public int cores() {
        return os.getAvailableProcessors();
    }

    public long maxMemoryMb() {
        return Runtime.getRuntime().maxMemory() / (1024 * 1024);
    }
}
