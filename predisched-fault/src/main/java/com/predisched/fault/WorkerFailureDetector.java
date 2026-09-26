package com.predisched.fault;

import com.predisched.common.obs.LamportInterceptors;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Declares a worker DEAD after {@code misses} heartbeat intervals without one (FR12, spec Exp 8:
 * three by default). What happens then, dropping it and re-queueing its RUNNING tasks with reason
 * WORKER_LOST, belongs to the scheduler behind {@link Workers}. Only the primary runs it; a worker
 * that comes back registers again as a new worker.
 */
public class WorkerFailureDetector implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerFailureDetector.class);

    /** The scheduler's registry, as the detector needs it. */
    public interface Workers {

        /** Every registered worker and when its last heartbeat arrived. */
        Map<String, Long> lastHeartbeats();

        /** Drops the worker and re-queues what was running on it. */
        void declareDead(String workerId, long silentMs, int missed);
    }

    private final Workers workers;
    private final long heartbeatIntervalMs;
    private final int misses;
    private final LongSupplier clock;
    private final ScheduledExecutorService timer;
    private ScheduledFuture<?> checks;

    public WorkerFailureDetector(
            Workers workers, long heartbeatIntervalMs, int misses, LongSupplier clock) {
        this.workers = workers;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.misses = Math.max(1, misses);
        this.clock = clock;
        this.timer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "worker-failure-detector");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Checks every half interval, so a death is noticed well within one more interval. */
    public synchronized void start() {
        if (checks != null) {
            return;
        }
        long period = Math.max(50, heartbeatIntervalMs / 2);
        checks = timer.scheduleWithFixedDelay(() -> {
            try {
                check();
            } catch (RuntimeException e) {
                log.warn("Worker failure check failed: {}", e.toString());
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if (checks != null) {
            checks.cancel(false);
            checks = null;
        }
    }

    /** One pass; returns how many workers were declared dead. */
    public int check() {
        LamportInterceptors.applyMdc();
        long now = clock.getAsLong();
        int dead = 0;
        for (Map.Entry<String, Long> worker : workers.lastHeartbeats().entrySet()) {
            long silentMs = now - worker.getValue();
            int missed = (int) (silentMs / heartbeatIntervalMs);
            if (missed >= misses) {
                workers.declareDead(worker.getKey(), silentMs, missed);
                dead++;
            }
        }
        return dead;
    }

    @Override
    public void close() {
        stop();
        timer.shutdownNow();
    }
}
