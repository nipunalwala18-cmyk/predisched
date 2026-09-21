package com.predisched.worker;

import com.predisched.common.obs.LamportInterceptors;
import com.predisched.proto.Ack;
import com.predisched.proto.Heartbeat;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.RegistryServiceGrpc;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers this worker with the scheduler and then reports metrics on a fixed interval (FR6, FR7).
 *
 * <p>Registration retries with exponential backoff, so a worker may be started before its
 * scheduler. If a heartbeat is refused because the scheduler has forgotten this worker (a restart),
 * the worker registers again.
 */
public class RegistrationClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RegistrationClient.class);

    private final RegistryServiceGrpc.RegistryServiceBlockingStub registry;
    private final ExecutionEngine engine;
    private final WorkerMetrics metrics;
    private final String workerId;
    private final String host;
    private final int port;
    private final long heartbeatIntervalMs;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean registered = new AtomicBoolean();

    public RegistrationClient(
            RegistryServiceGrpc.RegistryServiceBlockingStub registry,
            ExecutionEngine engine,
            WorkerMetrics metrics,
            String workerId,
            String host,
            int port,
            long heartbeatIntervalMs) {
        this.registry = registry;
        this.engine = engine;
        this.metrics = metrics;
        this.workerId = workerId;
        this.host = host;
        this.port = port;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, workerId + "-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Registers in the background, then heartbeats forever. */
    public void start() {
        scheduler.execute(this::registerWithBackoff);
        scheduler.scheduleAtFixedRate(
                this::sendHeartbeat, heartbeatIntervalMs, heartbeatIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    /** Blocking single attempt, used by tests and by the backoff loop. */
    boolean register() {
        LamportInterceptors.applyMdc();
        RegisterRequest request = RegisterRequest.newBuilder()
                .setWorkerId(workerId)
                .setHost(host)
                .setPort(port)
                .setCores(metrics.cores())
                .setMemoryMb(metrics.maxMemoryMb())
                .setPoolSize(engine.poolSize())
                .build();
        try {
            Ack ack = registry.register(request);
            if (ack.getOk()) {
                registered.set(true);
                log.info("Registered {} with scheduler: {}", workerId, ack.getMessage());
                return true;
            }
            log.warn("Scheduler refused registration of {}: {}", workerId, ack.getMessage());
        } catch (Exception e) {
            log.debug("Registration attempt for {} failed: {}", workerId, e.getMessage());
        }
        return false;
    }

    private void registerWithBackoff() {
        long delayMs = 200;
        while (!Thread.currentThread().isInterrupted() && !register()) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            delayMs = Math.min(delayMs * 2, 5000);
        }
    }

    void sendHeartbeat() {
        LamportInterceptors.applyMdc();
        if (!registered.get() && !register()) {
            return;
        }
        Heartbeat heartbeat = Heartbeat.newBuilder()
                .setWorkerId(workerId)
                .setCpuPct(metrics.cpuPct())
                .setMemPct(metrics.memPct())
                .setActiveThreads(engine.activeThreads())
                .setQueueLen(engine.queueLength())
                .setTasksCompleted(metrics.tasksCompleted())
                .setAvgExecMs(metrics.avgExecMs())
                .setLamportTime(0L)
                .build();
        try {
            Ack ack = registry.sendHeartbeat(heartbeat);
            if (!ack.getOk()) {
                log.info("Scheduler does not know {} ({}), registering again",
                        workerId, ack.getMessage());
                registered.set(false);
            }
        } catch (Exception e) {
            log.debug("Heartbeat from {} failed: {}", workerId, e.getMessage());
            registered.set(false);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
