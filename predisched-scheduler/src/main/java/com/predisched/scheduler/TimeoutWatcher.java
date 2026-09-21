package com.predisched.scheduler;

import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.time.Clocks;
import com.predisched.proto.CancelRequest;
import com.predisched.scheduler.queue.RunningTasks;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kills runaway tasks (FR26, F5). Every {@code checkIntervalMs} it looks for running tasks past
 * their deadline and tells the worker to stop them.
 *
 * <p>It deliberately does not decide what happens next. It marks the attempt as timed out and
 * cancels the execution; the dispatcher's own call then returns and records the attempt, so the
 * retry decision stays in one place.
 */
public class TimeoutWatcher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TimeoutWatcher.class);

    private final RunningTasks running;
    private final WorkerRegistry workers;
    private final WorkerStubs stubs;
    private final long checkIntervalMs;
    private final ScheduledExecutorService scheduler;

    public TimeoutWatcher(
            RunningTasks running,
            WorkerRegistry workers,
            WorkerStubs stubs,
            long checkIntervalMs) {
        this.running = running;
        this.workers = workers;
        this.stubs = stubs;
        this.checkIntervalMs = checkIntervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "timeout-watcher");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        if (checkIntervalMs <= 0) {
            return;
        }
        scheduler.scheduleAtFixedRate(() -> {
            try {
                check();
            } catch (Exception e) {
                log.warn("Timeout check failed: {}", e.getMessage());
            }
        }, checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);
    }

    /** One pass. Returns how many tasks were cancelled, for tests. */
    public int check() {
        LamportInterceptors.applyMdc();
        long now = Clocks.now();
        int cancelled = 0;
        for (RunningTasks.Running task : running.overdue(now)) {
            // compareAndSet: if two passes overlap, only one sends the cancellation.
            if (!task.timedOut().compareAndSet(false, true)) {
                continue;
            }
            long overdueMs = now - task.deadlineMs();
            log.warn("Task {} on {} passed its deadline by {} ms; cancelling attempt {}",
                    task.taskId(), task.workerId(), overdueMs, task.attempt());
            EventLog.get().event("TIMEOUT", task.taskId(), Map.of(
                    "worker", task.workerId(),
                    "attempt", String.valueOf(task.attempt()),
                    "overdue_ms", String.valueOf(overdueMs)));
            workers.get(task.workerId()).ifPresentOrElse(worker -> {
                try {
                    stubs.stubFor(worker).cancelExecution(CancelRequest.newBuilder()
                            .setTaskId(task.taskId())
                            .setReason("timeout")
                            .build());
                } catch (Exception e) {
                    log.warn("Could not cancel {} on {}: {}",
                            task.taskId(), worker.id(), e.getMessage());
                }
            }, () -> log.warn("Worker {} is gone; {} will be handled when its call returns",
                    task.workerId(), task.taskId()));
            cancelled++;
        }
        return cancelled;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
