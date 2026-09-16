package com.predisched.worker;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Live execution metrics for one worker.
 *
 * <p>All updates are thread-safe: counters use atomics and the rolling window is
 * guarded by its own monitor. OS load comes from
 * {@code com.sun.management.OperatingSystemMXBean} when available, otherwise -1.
 */
public class WorkerMetrics {

  /** Rolling window size for the mean execution time. */
  static final int WINDOW = 50;

  private final AtomicLong completed = new AtomicLong();
  private final AtomicInteger activeThreads = new AtomicInteger();
  private final Deque<Long> recentExecMs = new ArrayDeque<>(WINDOW);
  private final OperatingSystemMXBean osBean;

  public WorkerMetrics() {
    OperatingSystemMXBean bean = null;
    try {
      if (ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean sun) {
        bean = sun;
      }
    } catch (SecurityException e) {
      bean = null;
    }
    this.osBean = bean;
  }

  /** Record one finished task execution. */
  public void recordCompletion(long execMs) {
    completed.incrementAndGet();
    synchronized (recentExecMs) {
      if (recentExecMs.size() >= WINDOW) {
        recentExecMs.removeFirst();
      }
      recentExecMs.addLast(execMs);
    }
  }

  public void taskStarted() {
    activeThreads.incrementAndGet();
  }

  public void taskFinished() {
    activeThreads.decrementAndGet();
  }

  public long completedCount() {
    return completed.get();
  }

  public int activeThreads() {
    return activeThreads.get();
  }

  /** Mean execution time over the last {@value #WINDOW} tasks, or 0 if none. */
  public double meanExecMs() {
    synchronized (recentExecMs) {
      if (recentExecMs.isEmpty()) {
        return 0;
      }
      long sum = 0;
      for (long v : recentExecMs) {
        sum += v;
      }
      return (double) sum / recentExecMs.size();
    }
  }

  /** Host CPU load in percent, or -1 when unavailable. */
  public double cpuPct() {
    if (osBean == null) {
      return -1;
    }
    double load = osBean.getCpuLoad();
    return load < 0 ? -1 : load * 100.0;
  }

  /** Host memory usage in percent, or -1 when unavailable. */
  public double memPct() {
    if (osBean == null) {
      return -1;
    }
    long total = osBean.getTotalMemorySize();
    if (total <= 0) {
      return -1;
    }
    return (double) (total - osBean.getFreeMemorySize()) / total * 100.0;
  }
}
