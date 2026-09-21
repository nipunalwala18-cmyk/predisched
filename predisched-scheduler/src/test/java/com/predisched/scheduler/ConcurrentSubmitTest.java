package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Many clients submitting at once (lab Exp 2): the scheduler must accept every task exactly once,
 * dispatch each exactly once, and finish them all.
 */
class ConcurrentSubmitTest {

    private static final int CLIENT_THREADS = 8;
    private static final int TASKS_PER_CLIENT = 100;

    /** Counts how often each task id was executed, so a duplicate dispatch is visible. */
    static class CountingWorkerService extends WorkerServiceGrpc.WorkerServiceImplBase {
        final ConcurrentHashMap<String, AtomicInteger> executions = new ConcurrentHashMap<>();

        @Override
        public void executeTask(ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
            String taskId = request.getTask().getTaskId();
            executions.computeIfAbsent(taskId, id -> new AtomicInteger()).incrementAndGet();
            observer.onNext(ExecuteResult.newBuilder()
                    .setTaskId(taskId)
                    .setSuccess(true)
                    .setOutput("ok")
                    .setExecTimeMs(1L)
                    .build());
            observer.onCompleted();
        }
    }

    @Test
    void eightClientsSubmittingAtOnceLoseAndDuplicateNothing() throws Exception {
        String workerName = "worker-" + UUID.randomUUID();
        String schedulerName = "sched-" + UUID.randomUUID();
        TaskStore store = new InMemoryTaskStore();
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        CountingWorkerService worker = new CountingWorkerService();

        Server workerServer = InProcessServerBuilder.forName(workerName)
                .addService(worker).build().start();
        ManagedChannel workerChannel = InProcessChannelBuilder.forName(workerName).build();
        WorkerServiceGrpc.WorkerServiceBlockingStub workerStub =
                WorkerServiceGrpc.newBlockingStub(workerChannel);

        WorkerRegistry registry = new WorkerRegistry(60_000);
        registry.register(RegisterRequest.newBuilder()
                .setWorkerId("worker-1").setHost("in-process").setPort(0)
                .setCores(4).setMemoryMb(512).setPoolSize(4).build());

        Server schedulerServer = InProcessServerBuilder.forName(schedulerName)
                .addService(new SchedulerServiceImpl(store, new TaskValidator(4096), queue))
                .build().start();
        ManagedChannel schedulerChannel = InProcessChannelBuilder.forName(schedulerName).build();

        ExecutorService clients = Executors.newFixedThreadPool(CLIENT_THREADS);
        try (Dispatcher dispatcher =
                new Dispatcher(store, queue, registry, w -> workerStub, 4, 20)) {
            dispatcher.start();
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(CLIENT_THREADS);
            AtomicInteger accepted = new AtomicInteger();

            for (int c = 0; c < CLIENT_THREADS; c++) {
                final int client = c;
                clients.execute(() -> {
                    SchedulerServiceGrpc.SchedulerServiceBlockingStub stub =
                            SchedulerServiceGrpc.newBlockingStub(schedulerChannel);
                    try {
                        start.await();
                        for (int i = 0; i < TASKS_PER_CLIENT; i++) {
                            TaskResponse response = stub.submitTask(TaskRequest.newBuilder()
                                    .setTaskId("c" + client + "-t" + i)
                                    .setType(TaskType.SLEEP_TASK)
                                    .setInput("ms=1")
                                    .setPriority(5)
                                    .build());
                            if (response.getAccepted()) {
                                accepted.incrementAndGet();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(60, TimeUnit.SECONDS), "clients finished submitting");

            int expected = CLIENT_THREADS * TASKS_PER_CLIENT;
            assertEquals(expected, accepted.get(), "every submit accepted");

            long deadline = System.currentTimeMillis() + 60_000L;
            while (System.currentTimeMillis() < deadline) {
                long terminal = store.list().stream()
                        .filter(r -> r.status() == TaskStatus.COMPLETED
                                || r.status() == TaskStatus.FAILED)
                        .count();
                if (terminal == expected) {
                    break;
                }
                Thread.sleep(50L);
            }

            List<TaskRecord> records = store.list();
            assertEquals(expected, records.size(), "no task lost");
            Set<String> ids = new HashSet<>();
            for (TaskRecord record : records) {
                assertTrue(ids.add(record.id()), "duplicate record for " + record.id());
                assertEquals(TaskStatus.COMPLETED, record.status(),
                        record.id() + " ended as " + record.status());
            }
            assertEquals(expected, worker.executions.size(), "every task reached the worker");
            worker.executions.forEach((id, count) ->
                    assertEquals(1, count.get(), id + " was executed " + count.get() + " times"));
        } finally {
            clients.shutdownNow();
            schedulerChannel.shutdownNow();
            workerChannel.shutdownNow();
            schedulerServer.shutdownNow();
            workerServer.shutdownNow();
        }
    }
}
