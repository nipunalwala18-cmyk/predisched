package com.predisched.scheduler;

import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.WorkerServiceGrpc;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single dispatch thread taking task ids from a queue, calling the configured
 * worker's {@code ExecuteTask} and moving records {@code RUNNING -&gt;}
 * {@code COMPLETED}/{@code FAILED}. One worker for now; prompt 08 adds strategies.
 */
public class Dispatcher {

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    private final TaskStore store;
    private final BlockingQueue<String> queue;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub worker;
    private final String workerId;
    private volatile boolean running;
    private Thread thread;

    public Dispatcher(
            TaskStore store,
            BlockingQueue<String> queue,
            WorkerServiceGrpc.WorkerServiceBlockingStub worker,
            String workerId) {
        this.store = store;
        this.queue = queue;
        this.worker = worker;
        this.workerId = workerId;
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
                process(taskId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!running) {
                    return;
                }
            }
        }
    }

    void process(String taskId) {
        TaskRecord current = store.get(taskId);
        if (current == null || current.status() != TaskStatus.QUEUED) {
            return;
        }
        try {
            store.update(taskId, r -> r.withWorkerId(workerId).withStatus(TaskStatus.RUNNING));
        } catch (Exception e) {
            log.warn("Dispatch skipped for {}: {}", taskId, e.getMessage());
            return;
        }
        TaskRecord running = store.get(taskId);
        ExecuteRequest execRequest = ExecuteRequest.newBuilder()
                .setTask(TaskRequest.newBuilder()
                        .setTaskId(running.id())
                        .setType(running.type())
                        .setInput(running.input())
                        .setPriority(running.priority())
                        .setLamportTime(0L)
                        .build())
                .build();
        try {
            ExecuteResult result = worker.executeTask(execRequest);
            if (result.getSuccess()) {
                store.update(taskId, r -> r
                        .withResult(result.getOutput())
                        .withExecTimeMs(result.getExecTimeMs())
                        .withStatus(TaskStatus.COMPLETED));
            } else {
                store.update(taskId, r -> r
                        .withResult(result.getOutput())
                        .withExecTimeMs(result.getExecTimeMs())
                        .withStatus(TaskStatus.FAILED));
            }
        } catch (Exception e) {
            log.warn("Worker call failed for {}: {}", taskId, e.getMessage());
            try {
                store.update(taskId, r -> r
                        .withResult("worker error: " + e.getMessage())
                        .withStatus(TaskStatus.FAILED));
            } catch (Exception inner) {
                log.warn("Could not mark {} FAILED: {}", taskId, inner.getMessage());
            }
        }
    }
}
