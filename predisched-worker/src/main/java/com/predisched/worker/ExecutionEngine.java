package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.obs.TraceContext;
import com.predisched.proto.TaskType;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
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
 *
 * <p>Idempotent dispatch (prompt 10): a call may carry a dispatch id naming one attempt. The engine
 * runs each dispatch id at most once. A second call with the same id while it runs waits for that
 * run; after it finished, it gets the recorded outcome. That is what lets a newly promoted primary
 * re-attach to work the old primary started, or collect its result, without running it twice.
 */
public class ExecutionEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ExecutionEngine.class);

    /** What one task run produced, including how long it waited in the pool queue. */
    public record Outcome(
            boolean success, String output, long execMs, long waitMs, boolean rejected) {

        static Outcome rejected(String workerId) {
            return new Outcome(false, "worker " + workerId + " queue full", 0, 0, true);
        }

        static Outcome draining(String workerId) {
            return new Outcome(false, "worker " + workerId + " is draining", 0, 0, true);
        }
    }

    private final ExecutorRegistry registry;
    private final WorkerMetrics metrics;
    private final String workerId;
    private final ThreadPoolExecutor pool;
    private final int poolSize;
    /** Simulated slower hardware: executions are stretched to this many times their length. */
    private volatile double slowdown = 1.0;
    /** Draining (prompt 19): running and queued work finishes, new tasks are refused. */
    private volatile boolean draining;
    /** A task in flight: the pool's handle and the reply the scheduler is waiting for. */
    private record InFlight(Future<?> handle, CompletableFuture<Outcome> reply) {}

    /** Tasks queued or running, so a timeout or a cancellation can stop them (FR26). */
    private final ConcurrentHashMap<String, InFlight> inFlight = new ConcurrentHashMap<>();

    /** Where a dispatch stands, for {@code QueryExecution}. */
    public enum DispatchState { UNKNOWN, RUNNING, FINISHED }

    /** How many finished dispatches are remembered; older ones answer UNKNOWN. */
    static final int FINISHED_REMEMBERED = 10_000;

    /** Dispatches queued or running, by dispatch id. */
    private final ConcurrentHashMap<String, CompletableFuture<Outcome>> dispatches =
            new ConcurrentHashMap<>();

    /**
     * Outcomes of finished dispatches, oldest evicted first. Guarded by its own monitor
     * ({@code Collections.synchronizedMap}).
     */
    private final Map<String, Outcome> finished = Collections.synchronizedMap(
            new LinkedHashMap<>(256, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Outcome> eldest) {
                    return size() > FINISHED_REMEMBERED;
                }
            });

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

    /**
     * Runs one attempt at most once (see the class comment). An empty dispatch id runs the task
     * unconditionally, as before prompt 10.
     */
    public CompletableFuture<Outcome> submit(
            String taskId, String dispatchId, TaskType type, String input, String traceId) {
        if (dispatchId == null || dispatchId.isEmpty()) {
            return submit(taskId, type, input, traceId);
        }
        Outcome done = finished.get(dispatchId);
        if (done != null) {
            log.info("Dispatch {} already ran here: answering with its result, not running it again",
                    dispatchId);
            return CompletableFuture.completedFuture(done);
        }
        CompletableFuture<Outcome> mine = new CompletableFuture<>();
        CompletableFuture<Outcome> running = dispatches.putIfAbsent(dispatchId, mine);
        if (running != null) {
            log.info("Dispatch {} is already running here: the new call waits for that run",
                    dispatchId);
            return running;
        }
        // It may have finished between the first look and the claim.
        done = finished.get(dispatchId);
        if (done != null) {
            dispatches.remove(dispatchId, mine);
            return CompletableFuture.completedFuture(done);
        }
        submit(taskId, type, input, traceId).whenComplete((outcome, error) -> {
            // A rejection never ran, so the same attempt may be sent again.
            if (outcome != null && !outcome.rejected()) {
                finished.put(dispatchId, outcome);
            }
            dispatches.remove(dispatchId, mine);
            if (error != null) {
                mine.completeExceptionally(error);
            } else {
                mine.complete(outcome);
            }
        });
        return mine;
    }

    /**
     * Makes this worker behave like slower hardware (prompt 15, the dataset campaign's
     * heterogeneous workers): each execution is stretched to {@code factor} times its real length.
     * Simulated, since one machine has one kind of core; values below 1 are treated as 1.
     */
    public void setSlowdown(double factor) {
        this.slowdown = Math.max(1.0, factor);
    }

    public double slowdown() {
        return slowdown;
    }

    /** Finish what is running and queued, accept nothing new (prompt 19, chaos drain). */
    public void drain() {
        draining = true;
        log.warn("Worker {} draining: {} running, {} queued will finish; new tasks are refused",
                workerId, activeThreads(), queueLength());
    }

    public boolean isDraining() {
        return draining;
    }

    /** Waits out the simulated extra time; an interrupt (cancel, FR26) ends it early. */
    private void stretch(long startNanos) throws InterruptedException {
        double factor = slowdown;
        if (factor > 1.0) {
            long real = System.nanoTime() - startNanos;
            TimeUnit.NANOSECONDS.sleep((long) ((factor - 1.0) * real));
        }
    }

    /** Where a dispatch stands. */
    public DispatchState query(String dispatchId) {
        if (finished.containsKey(dispatchId)) {
            return DispatchState.FINISHED;
        }
        return dispatches.containsKey(dispatchId) ? DispatchState.RUNNING : DispatchState.UNKNOWN;
    }

    public Optional<Outcome> finishedOutcome(String dispatchId) {
        return Optional.ofNullable(finished.get(dispatchId));
    }

    /** Runs the task with a trace id in scope, so the worker's log lines join the client's. */
    public CompletableFuture<Outcome> submit(
            String taskId, TaskType type, String input, String traceId) {
        CompletableFuture<Outcome> future = new CompletableFuture<>();
        if (draining) {
            // Refused like a full queue: the scheduler re-queues it without spending a retry.
            log.info("Refused {}: worker {} is draining", taskId, workerId);
            future.complete(Outcome.draining(workerId));
            return future;
        }
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
                    stretch(startNanos);
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

    /** The registry's executors, for what a result says about itself (cacheable, profile). */
    public ExecutorRegistry registry() {
        return registry;
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
