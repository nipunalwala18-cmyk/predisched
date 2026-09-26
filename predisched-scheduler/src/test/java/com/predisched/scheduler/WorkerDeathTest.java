package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.fault.WorkerFailureDetector;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.Heartbeat;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerServiceGrpc;
import com.predisched.scheduler.queue.AgeingPriorityQueue;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RetryPolicy;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.queue.TaskQueue;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** FR12: three missed heartbeats and a worker's running tasks complete elsewhere. */
class WorkerDeathTest {

    /** Takes every task and never answers: a worker that hung or was cut off. */
    private static final class SilentWorker extends WorkerServiceGrpc.WorkerServiceImplBase {
        @Override
        public void executeTask(ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
            // No reply, ever.
        }
    }

    private static final class HealthyWorker extends WorkerServiceGrpc.WorkerServiceImplBase {
        @Override
        public void executeTask(ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
            observer.onNext(ExecuteResult.newBuilder().setTaskId(request.getTask().getTaskId())
                    .setSuccess(true).setOutput("ok").setExecTimeMs(1).build());
            observer.onCompleted();
        }
    }

    @Test
    void aWorkerSilentForThreeHeartbeatsIsDeclaredDeadAndItsTasksCompleteElsewhere()
            throws Exception {
        AtomicLong now = new AtomicLong(1_000_000);
        String silentName = "silent-" + UUID.randomUUID();
        String healthyName = "healthy-" + UUID.randomUUID();
        Server silent = InProcessServerBuilder.forName(silentName)
                .addService(new SilentWorker()).build().start();
        Server healthy = InProcessServerBuilder.forName(healthyName)
                .addService(new HealthyWorker()).build().start();
        ManagedChannel toSilent = InProcessChannelBuilder.forName(silentName).build();
        ManagedChannel toHealthy = InProcessChannelBuilder.forName(healthyName).build();

        TaskStore store = new InMemoryTaskStore();
        TaskQueue queue = new AgeingPriorityQueue(0.1, 10);
        RetryCoordinator retries = new RetryCoordinator(store, queue,
                new RetryPolicy(10, 50, 0, 3, 42), new DeadLetterQueue());
        RunningTasks running = new RunningTasks();
        WorkerRegistry workers = new WorkerRegistry(60_000, now::get);
        workers.register(worker("worker-a", 2));
        WorkerStubs stubs = worker -> WorkerServiceGrpc.newBlockingStub(
                worker.id().equals("worker-a") ? toSilent : toHealthy);
        Dispatcher dispatcher = new Dispatcher(store, queue, workers, stubs, retries, running,
                0L, 1.0, 4, 10);
        SchedulerFailover failover = new SchedulerFailover(store, queue, running, dispatcher,
                workers, stubs, retries, new WorkflowManager(store, queue, retries), null, 300);
        try (WorkerFailureDetector detector =
                new WorkerFailureDetector(failover, 1_000, 3, now::get)) {
            dispatcher.start();
            for (String id : List.of("t1", "t2")) {
                store.put(TaskRecord.createQueued(id, TaskType.SLEEP_TASK, "ms=10", 5));
                queue.add(id, 5, 0);
            }
            long deadline = System.currentTimeMillis() + 5_000;
            while (running.countFor("worker-a") < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertEquals(2, running.countFor("worker-a"), "both went to the only worker");

            // A second worker arrives and keeps heartbeating; worker-a falls silent.
            workers.register(worker("worker-b", 4));
            now.addAndGet(2_500);
            workers.heartbeat(Heartbeat.newBuilder().setWorkerId("worker-b").build());
            assertEquals(0, detector.check(), "two missed heartbeats are not enough");
            now.addAndGet(600);
            workers.heartbeat(Heartbeat.newBuilder().setWorkerId("worker-b").build());
            assertEquals(1, detector.check(), "the third one is");

            assertTrue(workers.get("worker-a").isEmpty(), "worker-a dropped from the registry");
            for (String id : List.of("t1", "t2")) {
                TaskRecord done = awaitStatus(store, id, TaskStatus.COMPLETED);
                assertEquals("worker-b", done.workerId());
                assertEquals(List.of(TaskAttempt.Outcome.WORKER_LOST, TaskAttempt.Outcome.SUCCEEDED),
                        done.attempts().stream().map(TaskAttempt::outcome).toList());
            }
        } finally {
            dispatcher.close();
            retries.close();
            toSilent.shutdownNow();
            toHealthy.shutdownNow();
            silent.shutdownNow();
            healthy.shutdownNow();
        }
    }

    @Test
    void aTaskChosenForAWorkerThatThenDiedGoesBackToTheQueueWithoutUsingAnAttempt() {
        TaskStore store = new InMemoryTaskStore();
        TaskQueue queue = new AgeingPriorityQueue(0.1, 10);
        RetryCoordinator retries = new RetryCoordinator(store, queue,
                new RetryPolicy(10, 50, 0, 3, 42), new DeadLetterQueue());
        WorkerRegistry workers = new WorkerRegistry(60_000);
        WorkerInfo chosen = workers.register(worker("worker-a", 2));
        Dispatcher dispatcher = new Dispatcher(store, queue, workers, worker -> null, retries,
                new RunningTasks(), 0L, 1.0, 1, 10);
        store.put(TaskRecord.createQueued("t", TaskType.SLEEP_TASK, "ms=10", 5));

        workers.suspect("worker-a");   // its call for another task just failed as unreachable
        dispatcher.process("t", chosen);

        assertEquals(TaskStatus.QUEUED, store.get("t").status());
        assertEquals(0, store.get("t").attemptCount());
        assertEquals(List.of("t"), queue.snapshot());
        dispatcher.close();
        retries.close();
    }

    private static RegisterRequest worker(String id, int pool) {
        return RegisterRequest.newBuilder().setWorkerId(id).setHost("in-process").setPort(0)
                .setCores(pool).setMemoryMb(512).setPoolSize(pool).build();
    }

    private static TaskRecord awaitStatus(TaskStore store, String id, TaskStatus status)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && store.get(id).status() != status) {
            Thread.sleep(5);
        }
        assertEquals(status, store.get(id).status(), id);
        return store.get(id);
    }
}
