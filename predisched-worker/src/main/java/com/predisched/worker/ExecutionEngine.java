package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.obs.TraceContext;
import com.predisched.proto.TaskType;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The worker's execution engine (lab Exp 2): a fixed thread pool with a bounded queue that runs
 * tasks concurrently.
 *
 * <p>Backpressure is explicit. When the queue is full the task is <em>rejected</em> rather than
 * queued forever or run on the caller's thread, so the scheduler learns immediately that this
 * worker is saturated and can place the task elsewhere.
 */
public class ExecutionEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ExecutionEngine.class);

    /** What one task run produced, including how long it waited in the pool queue. */
    public record Outcome(
            boolean success, String output, long execMs, long waitMs, boolean rejected) {

        static Outcome rejected(String workerId) {
            return new Outcome(false, "worker " + workerId + " queue full", 0, 0, true);
        }
    }

    private final ExecutorRegistry registry;
    private final WorkerMetrics metrics;
    private final String workerId;
    private final ThreadPoolExecutor pool;
    private final int poolSize;
    /** A task in flight: the pool's handle and the reply the scheduler is waiting for. */
    private record InFlight(Future<?> handle, CompletableFuture<Outcome> reply) {}

    /** Tasks queued or running, so a timeout or a cancellation can stop them (FR26). */
    private final ConcurrentHashMap<String, InFlight> inFlight = new ConcurrentHashMap<>();

    public ExecutionEngine(
            ExecutorRegistry registry,
            WorkerMetrics metrics,
            String workerId,
            int poolSize,
            int queueCapacity) {
        this.registry = registry;
        this.metrics = metrics;
        this.workerId = workerId;
        this.poolSize = poolSize;
        AtomicInteger threadNumber = new AtomicInteger();
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable,
                    workerId + "-exec-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.pool = new ThreadPoolExecutor(
                poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(queueCapacity), factory);
    }

    /**
     * Queues a task. The future always completes normally: a rejection is an {@link Outcome} with
     * {@code rejected=true}, never an exception across the gRPC boundary.
     */
    public CompletableFuture<Outcome> submit(String taskId, TaskType type, String input) {
        return submit(taskId, type, input, TraceContext.current());
    }

    /** Runs the task with a trace id in scope, so the worker's log lines join the client's. */
    public CompletableFuture<Outcome> submit(
            String taskId, TaskType type, String input, String traceId) {
        CompletableFuture<Outcome> future = new CompletableFuture<>();
        long queuedAtNanos = System.nanoTime();
        try {
            Future<?> handle = pool.submit(() -> {
                TraceContext.set(traceId);
                LamportInterceptors.applyMdc();
                long startNanos = System.nanoTime();
                long waitMs = (startNanos - queuedAtNanos) / 1_000_000L;
                EventLog.get().event(EventLog.EXECUTE_START, taskId, Map.of(
                        "task_type", type.name(),
                        "thread", Thread.currentThread().getName(),
                        "wait_ms", String.valueOf(waitMs)));
                boolean success = false;
                String output;
                long execMs = 0;
                try {
                    ExecutionResult result = registry.execute(type, input);
                    execMs = (System.nanoTime() - startNanos) / 1_000_000L;
                    success = result.success();
                    output = success ? result.output() : "error: " + result.errorMessage();
                    if (success) {
                        metrics.recordCompleted(execMs);
                    } else {
                        metrics.recordFailed(execMs);
                    }
                    log.info("Executed {} type={} on {} in {} ms (waited {} ms)",
                            taskId, type, Thread.currentThread().getName(), execMs, waitMs);
                } catch (Throwable t) {
                    // An interrupted or cancelled run lands here. The call still has to be
                    // answered, or the scheduler would wait for a reply that never comes.
                    execMs = (System.nanoTime() - startNanos) / 1_000_000L;
                    output = "error: " + (Thread.currentThread().isInterrupted()
                            ? "cancelled after " + execMs + " ms"
                            : String.valueOf(t));
                    metrics.recordFailed(execMs);
                    log.warn("Task {} ended abnormally after {} ms: {}", taskId, execMs, output);
                } finally {
                    EventLog.get().event(EventLog.EXECUTE_END, taskId, Map.of(
                            "success", String.valueOf(success),
                            "exec_ms", String.valueOf(execMs)));
                    TraceContext.clear();
                    inFlight.remove(taskId);
                }
                future.complete(new Outcome(success, output, execMs, waitMs, false));
            });
            inFlight.put(taskId, new InFlight(handle, future));
        } catch (RejectedExecutionException e) {
            metrics.recordRejected();
            log.warn("Rejected {}: pool queue full ({} waiting)", taskId, queueLength());
            future.complete(Outcome.rejected(workerId));
        }
        return future;
    }

    /**
     * Stops a queued or running task (FR26). A queued task never starts; a running one is
     * interrupted, and its executor notices through {@code Thread.interrupted()}.
     *
     * @return true if the task was still in flight
     */
    public boolean cancel(String taskId, String reason) {
        InFlight task = inFlight.remove(taskId);
        if (task == null) {
            return false;
        }
        boolean stopped = task.handle().cancel(true);
        metrics.recordCancelled();
        log.warn("Cancelled {} ({}), stopped={}", taskId, reason, stopped);
        // A task cancelled before it started never runs its body, so nothing else would answer
        // the waiting call. Answer it here.
        task.reply().complete(new Outcome(false, "error: cancelled (" + reason + ")", 0, 0, false));
        return true;
    }

    public int activeThreads() {
        return pool.getActiveCount();
    }

    public int queueLength() {
        return pool.getQueue().size();
    }

    public int poolSize() {
        return poolSize;
    }

    @Override
    public void close() {
        pool.shutdownNow();
    }
}
