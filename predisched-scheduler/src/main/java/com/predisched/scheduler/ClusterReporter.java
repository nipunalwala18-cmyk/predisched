package com.predisched.scheduler;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs one line per interval summarising what the heartbeats say about each worker (FR7, FR13).
 * This is how worker metrics are visible while the dashboard does not exist yet.
 */
public class ClusterReporter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClusterReporter.class);

    private final WorkerRegistry registry;
    private final long intervalMs;
    private final ScheduledExecutorService scheduler;

    public ClusterReporter(WorkerRegistry registry, long intervalMs) {
        this.registry = registry;
        this.intervalMs = intervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "cluster-reporter");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        if (intervalMs <= 0) {
            return;
        }
        scheduler.scheduleAtFixedRate(this::report, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    void report() {
        List<WorkerInfo> workers = registry.all();
        if (workers.isEmpty()) {
            log.info("Cluster: no workers registered");
            return;
        }
        long now = System.currentTimeMillis();
        for (WorkerInfo worker : workers) {
            log.info(String.format(Locale.ROOT,
                    "Cluster: %s %s cpu=%.1f%% mem=%.1f%% active=%d/%d queued=%d completed=%d "
                            + "avgExec=%.0fms",
                    worker.id(),
                    worker.isHealthy(now, 5_000) ? "UP" : "SILENT",
                    worker.cpuPct(), worker.memPct(),
                    worker.activeThreads(), worker.poolSize(), worker.queueLen(),
                    worker.tasksCompleted(), worker.avgExecMs()));
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
