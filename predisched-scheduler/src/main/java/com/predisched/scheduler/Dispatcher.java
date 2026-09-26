package com.predisched.scheduler;

import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskInputSpec;
import com.predisched.common.TaskStore;
import com.predisched.common.db.ExecutionRow;
import com.predisched.common.db.History;
import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.obs.TraceContext;
import com.predisched.common.time.Clocks;
import com.predisched.fault.InDoubtResolver;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.WorkerServiceGrpc;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.cache.ResultCache;
import com.predisched.scheduler.queue.TaskQueue;
import com.predisched.scheduler.strategy.DecisionLog;
import com.predisched.scheduler.strategy.RoundRobinStrategy;
import com.predisched.scheduler.strategy.SchedulingDecision;
import com.predisched.scheduler.strategy.SchedulingStrategy;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
 *
 * <p>Every call carries a dispatch id, {@code <taskId>#<attempt>}, so a worker never runs one
 * attempt twice (prompt 10). That is what lets a newly promoted primary {@link #reattach} to a
 * call the old primary made, or {@link #takeResult} of one that finished while no one listened.
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
    private volatile ResultCache cache;
    private volatile ArrivalRate arrivals = new ArrivalRate();

    /**
     * What the scheduler knew when it sent an attempt: the ML features of spec §12.1, written
     * with the attempt's outcome to {@code execution_history} (prompt 11). Boxed fields are null
     * when unknown.
     */
    private record Features(String strategy, Integer workerCores, Integer concurrent,
            Double cpuPct, Double memPct, Integer activeThreads, Integer queueLen,
            Double arrivalRate, Double avgExecRecent) {
        static final Features UNKNOWN =
                new Features(null, null, null, null, null, null, null, null, null);
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
            thread = null;
        }
    }

    public boolean isActive() {
        return active;
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
        History.get().decision(task.id(), current.name(), chosen.id(), scores.get(chosen.id()),
                scores, micros, Clocks.now());
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
                || last.outcome() == TaskAttempt.Outcome.REJECTED
                || last.outcome() == TaskAttempt.Outcome.WORKER_LOST;
        return troubled ? last.workerId() : null;
    }

    /** Stores cacheable results here (F6); null turns it off. */
    public void useResultCache(ResultCache resultCache) {
        this.cache = resultCache;
    }

    /** Where the arrival-rate feature comes from: the scheduler service's submits. */
    public void useArrivalRate(ArrivalRate rate) {
        this.arrivals = rate;
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
        if (registry.get(worker.id()).isEmpty() || registry.isSuspected(worker.id())) {
            // Chosen when it was handed to the dispatch pool; the worker has since been declared
            // dead or stopped answering (prompt 10). Back to the queue, no attempt used.
            log.info("Worker {} went away before {} was sent to it; re-queueing", worker.id(),
                    taskId);
            queue.add(taskId, current.priority(), current.submittedAt());
            return;
        }
        try {
            String placedBy = strategy.get().name();
            store.update(taskId, r -> r.withWorkerId(worker.id()).withStrategy(placedBy)
                    .withStatus(TaskStatus.RUNNING));
        } catch (Exception e) {
            log.warn("Dispatch skipped for {}: {}", taskId, e.getMessage());
            return;
        }

        TaskRecord record = store.get(taskId);
        execute(record, worker, record.attemptCount() + 1, false);
    }

    /**
     * A task the old primary left running on {@code worker} (prompt 10): send the same dispatch
     * again. The worker recognises the dispatch id and answers when that run ends, so the task is
     * not run twice and its outcome is recorded here as usual.
     */
    public void reattach(TaskRecord record, WorkerInfo worker, int attempt) {
        dispatchPool.execute(() -> {
            LamportInterceptors.applyMdc();
            execute(record, worker, attempt, true);
        });
    }

    /** A dispatch that finished while no primary was listening: record its outcome now. */
    public void takeResult(TaskRecord record, int attempt, ExecuteResult result) {
        long now = Clocks.now();
        RunningTasks.Running entry = running.start(record.id(), record.workerId(), attempt, now, 0L);
        if (running.finish(entry)) {
            recordOutcome(record.id(), record.workerId(), entry, attempt, now, result,
                    Features.UNKNOWN);
        }
    }

    private void execute(TaskRecord record, WorkerInfo worker, int attemptNumber,
            boolean reattach) {
        String taskId = record.id();
        long startedAtMs = Clocks.now();
        long timeoutMs = record.timeoutMs() > 0 ? record.timeoutMs() : defaultTimeoutMs;
        Features features = featuresAt(worker);
        RunningTasks.Running inFlight = running.start(
                taskId, worker.id(), attemptNumber, startedAtMs,
                timeoutMs > 0 ? startedAtMs + timeoutMs : 0L);

        TraceContext.set(record.traceId());
        String dispatchId = InDoubtResolver.dispatchId(taskId, attemptNumber);
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
                .setDispatchId(dispatchId)
                .build();
        if (reattach) {
            EventLog.get().event("REATTACH", taskId, Map.of(
                    "worker", worker.id(), "dispatch", dispatchId));
            log.info("Re-attaching to {} on {} (dispatch {})", taskId, worker.id(), dispatchId);
        } else {
            EventLog.get().event(EventLog.DISPATCH, taskId, Map.of(
                    "worker", worker.id(),
                    "attempt", String.valueOf(attemptNumber)));
            log.info("Dispatching {} to {} (attempt {})", taskId, worker.id(), attemptNumber);
        }

        try {
            WorkerServiceGrpc.WorkerServiceBlockingStub stub = clients.stubFor(worker);
            ExecuteResult result = stub.executeTask(execRequest);
            if (!running.finish(inFlight)) {
                log.info("Reply for {} from {} ignored: the attempt was already settled",
                        taskId, worker.id());
                return;
            }
            recordOutcome(taskId, worker.id(), inFlight, attemptNumber, startedAtMs, result,
                    features);
        } catch (Exception e) {
            if (!running.finish(inFlight)) {
                // Settled already: the worker was declared dead and the task re-queued.
                return;
            }
            log.warn("Worker {} call failed for {}: {}", worker.id(), taskId, e.getMessage());
            if (e instanceof StatusRuntimeException status
                    && status.getStatus().getCode() == Status.Code.UNAVAILABLE) {
                // Probably gone: stop sending it work until it heartbeats again (prompt 10).
                registry.suspect(worker.id());
            }
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

    /** The worker's state and the scheduler's load as this attempt is sent (spec §12.1). */
    private Features featuresAt(WorkerInfo chosen) {
        WorkerInfo worker = registry.get(chosen.id()).orElse(chosen);
        return new Features(strategy.get().name(), worker.cores(), running.countFor(worker.id()),
                worker.cpuPct(), worker.memPct(), worker.activeThreads(), worker.queueLen(),
                arrivals.perSecond(), worker.avgExecMs());
    }

    private void recordOutcome(
            String taskId,
            String workerId,
            RunningTasks.Running inFlight,
            int attemptNumber,
            long startedAtMs,
            ExecuteResult result,
            Features features) {
        long endedAtMs = Clocks.now();
        recordHistory(taskId, workerId, attemptNumber, startedAtMs, result, features);
        EventLog.get().event(EventLog.RESULT, taskId, Map.of(
                "worker", workerId,
                "attempt", String.valueOf(attemptNumber),
                "success", String.valueOf(result.getSuccess()),
                "exec_ms", String.valueOf(result.getExecTimeMs()),
                "wait_ms", String.valueOf(result.getWaitTimeMs())));

        if (result.getRejected()) {
            log.info("Worker {} rejected {} (queue full), re-queueing", workerId, taskId);
            TaskAttempt attempt = new TaskAttempt(
                    attemptNumber, workerId, TaskAttempt.Outcome.REJECTED,
                    "worker queue full", startedAtMs, endedAtMs, 0L);
            safeFail(taskId, attempt, result.getOutput());
            return;
        }
        if (result.getSuccess()) {
            ResultCache resultCache = cache;
            TaskRecord finished = store.get(taskId);
            if (resultCache != null && result.getCacheable() && finished != null) {
                resultCache.store(finished.type(), finished.input(), result.getOutput());
            }
            TaskAttempt attempt = new TaskAttempt(
                    attemptNumber, workerId, TaskAttempt.Outcome.SUCCEEDED, "",
                    startedAtMs, endedAtMs, result.getExecTimeMs());
            retries.succeeded(taskId, attempt, result.getOutput(), result.getExecTimeMs());
            return;
        }

        boolean timedOut = inFlight.timedOut().get();
        TaskAttempt attempt = new TaskAttempt(
                attemptNumber, workerId,
                timedOut ? TaskAttempt.Outcome.TIMED_OUT : TaskAttempt.Outcome.FAILED,
                timedOut ? "TIMEOUT" : result.getOutput(),
                startedAtMs, endedAtMs, result.getExecTimeMs());
        safeFail(taskId, attempt, result.getOutput());
    }

    /** One {@code execution_history} row per attempt that ran (a rejection never ran). */
    private void recordHistory(String taskId, String workerId, int attempt, long dispatchedAtMs,
            ExecuteResult result, Features f) {
        if (result.getRejected()) {
            return;
        }
        TaskRecord task = store.get(taskId);
        if (task == null) {
            return;
        }
        History.get().execution(new ExecutionRow(taskId, task.type().name(),
                result.getResourceProfile().isEmpty() ? null : result.getResourceProfile(),
                attempt, result.getSuccess() ? "COMPLETED" : "FAILED", f.strategy(),
                task.priority(), TaskInputSpec.inputSize(task.type(), task.input()), workerId,
                f.workerCores(), f.concurrent(), f.cpuPct(), f.memPct(), f.activeThreads(),
                f.queueLen(), f.arrivalRate(), f.avgExecRecent(), dispatchedAtMs,
                result.getWaitTimeMs(), result.getExecTimeMs()));
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
