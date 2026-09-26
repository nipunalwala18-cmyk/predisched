package com.predisched.scheduler;

import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.obs.TraceContext;
import com.predisched.common.time.Clocks;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.WorkerServiceGrpc;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.queue.TaskQueue;
import com.predisched.scheduler.strategy.DecisionLog;
import com.predisched.scheduler.strategy.RoundRobinStrategy;
import com.predisched.scheduler.strategy.SchedulingDecision;
import com.predisched.scheduler.strategy.SchedulingStrategy;
import java.util.ArrayList;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes tasks off the queue and sends them to a registered worker.
 *
 * <p>One thread owns the queue and the gRPC calls run on a small pool, so a long task does not
 * block everything behind it (lab Exp 2). Each call is one <em>attempt</em>: the dispatcher records
 * how it ended and hands the outcome to the {@link RetryCoordinator}, which decides between a retry
 * and the dead-letter queue (F4).
 *
 * <p>Which worker gets a task is a {@link SchedulingStrategy} (prompt 08), held in an
 * {@link AtomicReference} so it can be swapped while running. Every choice is recorded as a
 * {@link SchedulingDecision} in the {@link DecisionLog} and the event log.
 */
public class Dispatcher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    private final TaskStore store;
    private final TaskQueue queue;
    private final WorkerRegistry registry;
    private final WorkerStubs clients;
    private final RetryCoordinator retries;
    private final RunningTasks running;
    private final long defaultTimeoutMs;
    private final double outstandingPerWorkerFactor;
    private final ExecutorService dispatchPool;
    private final long noWorkerRetryMs;
    private final AtomicReference<SchedulingStrategy> strategy;
    private final DecisionLog decisions = new DecisionLog(1000);
    private volatile boolean active;
    private Thread thread;

    public Dispatcher(
            TaskStore store,
            TaskQueue queue,
            WorkerRegistry registry,
            WorkerStubs clients,
            RetryCoordinator retries,
            RunningTasks running,
            long defaultTimeoutMs,
            double outstandingPerWorkerFactor,
            int dispatchThreads,
            long noWorkerRetryMs) {
        this(store, queue, registry, clients, retries, running, defaultTimeoutMs,
                outstandingPerWorkerFactor, dispatchThreads, noWorkerRetryMs,
                new RoundRobinStrategy());
    }

    public Dispatcher(
            TaskStore store,
            TaskQueue queue,
            WorkerRegistry registry,
            WorkerStubs clients,
            RetryCoordinator retries,
            RunningTasks running,
            long defaultTimeoutMs,
            double outstandingPerWorkerFactor,
            int dispatchThreads,
            long noWorkerRetryMs,
            SchedulingStrategy strategy) {
        this.strategy = new AtomicReference<>(strategy);
        this.store = store;
        this.queue = queue;
        this.registry = registry;
        this.clients = clients;
        this.retries = retries;
        this.running = running;
        this.defaultTimeoutMs = defaultTimeoutMs;
        this.outstandingPerWorkerFactor = outstandingPerWorkerFactor;
        this.noWorkerRetryMs = noWorkerRetryMs;
        AtomicInteger n = new AtomicInteger();
        this.dispatchPool = Executors.newFixedThreadPool(dispatchThreads, runnable -> {
            Thread t = new Thread(runnable, "dispatch-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    public synchronized void start() {
        if (active) {
            return;
        }
        active = true;
        thread = new Thread(this::loop, "dispatcher");
        thread.setDaemon(true);
        thread.start();
    }

    public synchronized void stop() {
        active = false;
        if (thread != null) {
            thread.interrupt();
        }
    }

    private void loop() {
        LamportInterceptors.applyMdc();
        while (active) {
            try {
                String taskId = queue.take();
                TaskRecord next = store.get(taskId);
                if (next == null) {
                    continue;
                }
                Optional<WorkerInfo> worker = chooseWorker(next);
                if (worker.isEmpty()) {
                    // Nothing to dispatch to yet: put it back and pause rather than spin.
                    queue.add(taskId, next.priority(), next.submittedAt());
                    Thread.sleep(noWorkerRetryMs);
                    continue;
                }
                WorkerInfo chosen = worker.get();
                // Logged on the queue thread: the dispatch pool finishes tasks out of order, so
                // this is the only place that shows what the queue actually chose next (F2).
                log.info("Next from queue: {} (priority {}, waited {} ms, {} still queued)",
                        taskId,
                        next == null ? "?" : next.priority(),
                        next == null ? 0 : Clocks.now() - next.submittedAt(),
                        queue.size());
                dispatchPool.execute(() -> process(taskId, chosen));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!active) {
                    return;
                }
            }
        }
    }

    /**
     * Asks the strategy to pick among the healthy workers with spare capacity, and records the
     * decision. Empty when no worker has room.
     *
     * <p>Capacity matters for more than politeness: a worker accepts far more tasks than it can
     * run at once (its pool queue holds 100), so dispatching eagerly would empty the scheduler's
     * queue into the worker's, and the scheduler's priority ordering would decide nothing. Holding
     * work back until a worker has room is what makes priority and ageing real (F2). The count
     * comes from this scheduler's own in-flight map, so it is exact and immediate.
     */
    Optional<WorkerInfo> chooseWorker(TaskRecord task) {
        List<WorkerInfo> candidates = candidates(task);
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        SchedulingStrategy current = strategy.get();
        long started = System.nanoTime();
        WorkerInfo chosen = current.select(task, candidates);
        long micros = (System.nanoTime() - started) / 1_000;
        Map<String, Double> scores = current.scores(task, candidates);
        decisions.add(new SchedulingDecision(
                task.id(), current.name(), chosen.id(), scores, micros, Clocks.now()));
        EventLog.get().event(EventLog.SCHEDULE_DECISION, task.id(), Map.of(
                "strategy", current.name(),
                "worker", chosen.id(),
                "candidates", String.valueOf(candidates.size()),
                "decision_us", String.valueOf(micros),
                "scores", scores.entrySet().stream()
                        .map(entry -> entry.getKey() + "="
                                + String.format(Locale.ROOT, "%.3f", entry.getValue()))
                        .collect(Collectors.joining(" "))));
        return Optional.of(chosen);
    }

    /**
     * Healthy workers under their outstanding limit, their load raised to this scheduler's
     * in-flight count. A task whose last attempt timed out or was rejected avoids that worker
     * when another one has room, so a retry is not sent straight back to the same trouble.
     */
    List<WorkerInfo> candidates(TaskRecord task) {
        List<WorkerInfo> open = new ArrayList<>();
        for (WorkerInfo worker : registry.healthy()) {
            int limit = Math.max(1, (int) Math.round(worker.poolSize() * outstandingPerWorkerFactor));
            int inFlight = running.countFor(worker.id());
            if (inFlight < limit) {
                open.add(worker.withLiveLoad(inFlight));
            }
        }
        String avoid = lastTroubledWorker(task);
        if (avoid != null && open.size() > 1) {
            List<WorkerInfo> others = open.stream()
                    .filter(worker -> !worker.id().equals(avoid))
                    .toList();
            if (!others.isEmpty()) {
                return others;
            }
        }
        return open;
    }

    private static String lastTroubledWorker(TaskRecord task) {
        if (task.attempts().isEmpty()) {
            return null;
        }
        TaskAttempt last = task.attempts().get(task.attempts().size() - 1);
        boolean troubled = last.outcome() == TaskAttempt.Outcome.TIMED_OUT
                || last.outcome() == TaskAttempt.Outcome.REJECTED;
        return troubled ? last.workerId() : null;
    }

    /** Swaps the strategy for every later dispatch (the admin API in prompt 22 calls this). */
    public void setStrategy(SchedulingStrategy next) {
        SchedulingStrategy previous = strategy.getAndSet(next);
        log.info("Scheduling strategy changed from {} to {}", previous.name(), next.name());
    }

    public SchedulingStrategy strategy() {
        return strategy.get();
    }

    public DecisionLog decisions() {
        return decisions;
    }

    void process(String taskId, WorkerInfo worker) {
        LamportInterceptors.applyMdc();
        TaskRecord current = store.get(taskId);
        if (current == null || current.status() != TaskStatus.QUEUED) {
            return;
        }
        try {
            store.update(taskId, r -> r.withWorkerId(worker.id()).withStatus(TaskStatus.RUNNING));
        } catch (Exception e) {
            log.warn("Dispatch skipped for {}: {}", taskId, e.getMessage());
            return;
        }

        TaskRecord record = store.get(taskId);
        int attemptNumber = record.attemptCount() + 1;
        long startedAtMs = Clocks.now();
        long timeoutMs = record.timeoutMs() > 0 ? record.timeoutMs() : defaultTimeoutMs;
        RunningTasks.Running inFlight = running.start(
                taskId, worker.id(), attemptNumber, startedAtMs,
                timeoutMs > 0 ? startedAtMs + timeoutMs : 0L);

        TraceContext.set(record.traceId());
        ExecuteRequest execRequest = ExecuteRequest.newBuilder()
                .setTask(TaskRequest.newBuilder()
                        .setTaskId(record.id())
                        .setType(record.type())
                        .setInput(record.input())
                        .setPriority(record.priority())
                        .setLamportTime(Clocks.lamport().current())
                        .setTraceId(record.traceId())
                        .setTimeoutMs(timeoutMs)
                        .build())
                .build();
        EventLog.get().event(EventLog.DISPATCH, taskId, Map.of(
                "worker", worker.id(),
                "attempt", String.valueOf(attemptNumber)));
        log.info("Dispatching {} to {} (attempt {})", taskId, worker.id(), attemptNumber);

        try {
            WorkerServiceGrpc.WorkerServiceBlockingStub stub = clients.stubFor(worker);
            ExecuteResult result = stub.executeTask(execRequest);
            running.finish(taskId);
            recordOutcome(taskId, worker, inFlight, attemptNumber, startedAtMs, result);
        } catch (Exception e) {
            running.finish(taskId);
            log.warn("Worker {} call failed for {}: {}", worker.id(), taskId, e.getMessage());
            TaskAttempt attempt = new TaskAttempt(
                    attemptNumber, worker.id(),
                    inFlight.timedOut().get()
                            ? TaskAttempt.Outcome.TIMED_OUT
                            : TaskAttempt.Outcome.WORKER_LOST,
                    e.getMessage() == null ? "worker call failed" : e.getMessage(),
                    startedAtMs, Clocks.now(), 0L);
            safeFail(taskId, attempt, "worker error: " + e.getMessage());
        } finally {
            TraceContext.clear();
        }
    }

    private void recordOutcome(
            String taskId,
            WorkerInfo worker,
            RunningTasks.Running inFlight,
            int attemptNumber,
            long startedAtMs,
            ExecuteResult result) {
        long endedAtMs = Clocks.now();
        EventLog.get().event(EventLog.RESULT, taskId, Map.of(
                "worker", worker.id(),
                "attempt", String.valueOf(attemptNumber),
                "success", String.valueOf(result.getSuccess()),
                "exec_ms", String.valueOf(result.getExecTimeMs()),
                "wait_ms", String.valueOf(result.getWaitTimeMs())));

        if (result.getRejected()) {
            log.info("Worker {} rejected {} (queue full), re-queueing", worker.id(), taskId);
            TaskAttempt attempt = new TaskAttempt(
                    attemptNumber, worker.id(), TaskAttempt.Outcome.REJECTED,
                    "worker queue full", startedAtMs, endedAtMs, 0L);
            safeFail(taskId, attempt, result.getOutput());
            return;
        }
        if (result.getSuccess()) {
            TaskAttempt attempt = new TaskAttempt(
                    attemptNumber, worker.id(), TaskAttempt.Outcome.SUCCEEDED, "",
                    startedAtMs, endedAtMs, result.getExecTimeMs());
            retries.succeeded(taskId, attempt, result.getOutput(), result.getExecTimeMs());
            return;
        }

        boolean timedOut = inFlight.timedOut().get();
        TaskAttempt attempt = new TaskAttempt(
                attemptNumber, worker.id(),
                timedOut ? TaskAttempt.Outcome.TIMED_OUT : TaskAttempt.Outcome.FAILED,
                timedOut ? "TIMEOUT" : result.getOutput(),
                startedAtMs, endedAtMs, result.getExecTimeMs());
        safeFail(taskId, attempt, result.getOutput());
    }

    /**
     * Failure handling runs on the dispatch pool, so it must never throw. The record is left
     * RUNNING here: the coordinator moves it to QUEUED for a retry or FAILED when retries run out,
     * which are the only legal transitions from RUNNING.
     */
    private void safeFail(String taskId, TaskAttempt attempt, String output) {
        try {
            retries.failed(taskId, attempt, output);
        } catch (Exception e) {
            log.warn("Could not record failure of {}: {}", taskId, e.getMessage());
        }
    }

    @Override
    public void close() {
        stop();
        dispatchPool.shutdownNow();
        try {
            dispatchPool.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
