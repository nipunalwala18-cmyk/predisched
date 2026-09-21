package com.predisched.scheduler;

import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.TraceContext;
import com.predisched.common.time.Clocks;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.WorkerServiceGrpc;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes task ids off the queue and sends them to a registered worker.
 *
 * <p>One thread owns the queue, but the gRPC calls run on a small pool, so a long task no longer
 * blocks everything behind it (lab Exp 2). Worker choice is still "the first healthy one"; real
 * strategies arrive in prompt 08.
 */
public class Dispatcher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    private final TaskStore store;
    private final BlockingQueue<String> queue;
    private final WorkerRegistry registry;
    private final WorkerStubs clients;
    private final ExecutorService dispatchPool;
    private final long noWorkerRetryMs;
    private volatile boolean running;
    private Thread thread;

    public Dispatcher(
            TaskStore store,
            BlockingQueue<String> queue,
            WorkerRegistry registry,
            WorkerStubs clients,
            int dispatchThreads,
            long noWorkerRetryMs) {
        this.store = store;
        this.queue = queue;
        this.registry = registry;
        this.clients = clients;
        this.noWorkerRetryMs = noWorkerRetryMs;
        AtomicInteger n = new AtomicInteger();
        this.dispatchPool = Executors.newFixedThreadPool(dispatchThreads, runnable -> {
            Thread t = new Thread(runnable, "dispatch-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(this::loop, "dispatcher");
        thread.setDaemon(true);
        thread.start();
    }

    public synchronized void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
    }

    private void loop() {
        while (running) {
            try {
                String taskId = queue.take();
                Optional<WorkerInfo> worker = chooseWorker();
                if (worker.isEmpty()) {
                    // Nothing to dispatch to yet: put it back and pause briefly rather than spin.
                    queue.put(taskId);
                    Thread.sleep(noWorkerRetryMs);
                    continue;
                }
                WorkerInfo chosen = worker.get();
                dispatchPool.execute(() -> process(taskId, chosen));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!running) {
                    return;
                }
            }
        }
    }

    /** First healthy worker by id. Prompt 08 replaces this with pluggable strategies. */
    Optional<WorkerInfo> chooseWorker() {
        List<WorkerInfo> healthy = registry.healthy();
        return healthy.isEmpty() ? Optional.empty() : Optional.of(healthy.get(0));
    }

    void process(String taskId, WorkerInfo worker) {
        com.predisched.common.obs.LamportInterceptors.applyMdc();
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
        TaskRecord running = store.get(taskId);
        TraceContext.set(running.traceId());
        ExecuteRequest execRequest = ExecuteRequest.newBuilder()
                .setTask(TaskRequest.newBuilder()
                        .setTaskId(running.id())
                        .setType(running.type())
                        .setInput(running.input())
                        .setPriority(running.priority())
                        .setLamportTime(Clocks.lamport().current())
                        .setTraceId(running.traceId())
                        .build())
                .build();
        EventLog.get().event(EventLog.DISPATCH, taskId, Map.of("worker", worker.id()));
        log.info("Dispatching {} to {}", taskId, worker.id());
        try {
            WorkerServiceGrpc.WorkerServiceBlockingStub stub = clients.stubFor(worker);
            ExecuteResult result = stub.executeTask(execRequest);
            if (result.getRejected()) {
                log.info("Worker {} rejected {} (queue full), re-queueing", worker.id(), taskId);
                store.update(taskId, r -> r.withStatus(TaskStatus.QUEUED));
                queue.put(taskId);
                return;
            }
            store.update(taskId, r -> r
                    .withResult(result.getOutput())
                    .withExecTimeMs(result.getExecTimeMs())
                    .withStatus(result.getSuccess() ? TaskStatus.COMPLETED : TaskStatus.FAILED));
            EventLog.get().event(EventLog.RESULT, taskId, Map.of(
                    "worker", worker.id(),
                    "success", String.valueOf(result.getSuccess()),
                    "exec_ms", String.valueOf(result.getExecTimeMs()),
                    "wait_ms", String.valueOf(result.getWaitTimeMs())));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("Worker {} call failed for {}: {}", worker.id(), taskId, e.getMessage());
            try {
                store.update(taskId, r -> r
                        .withResult("worker error: " + e.getMessage())
                        .withStatus(TaskStatus.FAILED));
            } catch (Exception inner) {
                log.warn("Could not mark {} FAILED: {}", taskId, inner.getMessage());
            }
        } finally {
            TraceContext.clear();
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
