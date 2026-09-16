package com.predisched.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class WorkerMetricsTest {

  @Test
  void countsAreCorrectUnderConcurrency() throws Exception {
    WorkerMetrics metrics = new WorkerMetrics();
    int threads = 8;
    int perThread = 1000;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    for (int t = 0; t < threads; t++) {
      final long base = (long) t * perThread;
      pool.execute(
          () -> {
            try {
              start.await();
              for (int i = 0; i < perThread; i++) {
                metrics.taskStarted();
                metrics.recordCompletion(base + i);
                metrics.taskFinished();
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              done.countDown();
            }
          });
    }
    start.countDown();
    assertTrue(done.await(30, TimeUnit.SECONDS));
    pool.shutdownNow();

    assertEquals((long) threads * perThread, metrics.completedCount());
    assertEquals(0, metrics.activeThreads());
    // Window holds the last 50 values; the mean must be within the observed range.
    double mean = metrics.meanExecMs();
    assertTrue(mean >= 0 && mean < (long) threads * perThread, "mean=" + mean);
  }

  @Test
  void rollingMeanCoversLastFifty() {
    WorkerMetrics metrics = new WorkerMetrics();
    for (long i = 1; i <= 60; i++) {
      metrics.recordCompletion(i);
    }
    // Last 50 values are 11..60, mean = 35.5.
    assertEquals(35.5, metrics.meanExecMs(), 1e-9);
    assertEquals(60, metrics.completedCount());
  }

  @Test
  void emptyMeanIsZero() {
    assertEquals(0, new WorkerMetrics().meanExecMs(), 0.0);
  }
}
