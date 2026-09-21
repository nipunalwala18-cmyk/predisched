package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end scheduler flow over in-process gRPC. The worker here is a fake
 * {@code WorkerService} defined locally in the test so the scheduler module
 * never imports worker classes (they talk over gRPC only); real executors are
 * covered by worker-module tests and the live acceptance demo.
 */
public class SchedulerIntegrationTest {

    /** Fake worker: echoes a canned success for any task, like a real executor would. */
    static class FakeWorkerService extends WorkerServiceGrpc.WorkerServiceImplBase {
        @Override
        public void executeTask(ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
            observer.onNext(ExecuteResult.newBuilder()
                    .setTaskId(request.getTask().getTaskId())
                    .setSuccess(true)
                    .setOutput("fake-executed:" + request.getTask().getType()
                            + ":" + request.getTask().getInput())
                    .setExecTimeMs(2L)
                    .setWaitTimeMs(0L)
                    .setLamportTime(0L)
                    .build());
            observer.onCompleted();
        }
    }

    private String workerName;
    private String schedulerName;
    private Server workerServer;
    private Server schedulerServer;
    private ManagedChannel workerChannel;
    private ManagedChannel schedulerChannel;
    private SchedulerServiceGrpc.SchedulerServiceBlockingStub client;
    private Dispatcher dispatcher;

    @BeforeEach
    public void setUp() throws Exception {
        workerName = "fake-worker-" + UUID.randomUUID();
        schedulerName = "sched-" + UUID.randomUUID();
        TaskStore store = new InMemoryTaskStore();
        TaskValidator validator = new TaskValidator(4096);
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();

        workerServer = InProcessServerBuilder.forName(workerName)
                .addService(new FakeWorkerService())
                .directExecutor()
                .build()
                .start();
        workerChannel = InProcessChannelBuilder.forName(workerName).directExecutor().build();
        WorkerServiceGrpc.WorkerServiceBlockingStub workerStub =
                WorkerServiceGrpc.newBlockingStub(workerChannel);

        WorkerRegistry workers = new WorkerRegistry(60_000);
        workers.register(com.predisched.proto.RegisterRequest.newBuilder()
                .setWorkerId("worker-1")
                .setHost("in-process")
                .setPort(0)
                .setCores(4)
                .setMemoryMb(512)
                .setPoolSize(4)
                .build());
        dispatcher = new Dispatcher(store, queue, workers, worker -> workerStub, 2, 50);
        dispatcher.start();

        SchedulerServiceImpl service = new SchedulerServiceImpl(store, validator, queue);
        schedulerServer = InProcessServerBuilder.forName(schedulerName)
                .addService(service)
                .directExecutor()
                .build()
                .start();
        schedulerChannel = InProcessChannelBuilder.forName(schedulerName).directExecutor().build();
        client = SchedulerServiceGrpc.newBlockingStub(schedulerChannel);
    }

    @AfterEach
    public void tearDown() {
        if (dispatcher != null) {
            dispatcher.stop();
        }
        if (schedulerServer != null) {
            schedulerServer.shutdownNow();
        }
        if (workerServer != null) {
            workerServer.shutdownNow();
        }
        if (schedulerChannel != null) {
            schedulerChannel.shutdownNow();
        }
        if (workerChannel != null) {
            workerChannel.shutdownNow();
        }
    }

    private TaskStatusResponse waitForTerminal(String taskId) {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (true) {
            TaskStatusResponse status = client.getTaskStatus(
                    TaskStatusRequest.newBuilder().setTaskId(taskId).build());
            if (status.getStatus() == TaskStatus.COMPLETED
                    || status.getStatus() == TaskStatus.FAILED
                    || status.getStatus() == TaskStatus.CANCELLED) {
                return status;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("Timed out waiting for task " + taskId);
            }
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted waiting for task " + taskId);
            }
        }
    }

    @Test
    public void submitOneTaskOfEachTypeCompletes() {
        String[][] cases = {
            {"it-cpu-" + UUID.randomUUID(), "CPU_TASK", "n=50"},
            {"it-sleep-" + UUID.randomUUID(), "SLEEP_TASK", "ms=5"},
            {"it-matrix-" + UUID.randomUUID(), "MATRIX_TASK", "size=5"},
        };
        for (String[] c : cases) {
            TaskResponse response = client.submitTask(com.predisched.proto.TaskRequest.newBuilder()
                    .setTaskId(c[0])
                    .setType(TaskType.valueOf(c[1]))
                    .setInput(c[2])
                    .setPriority(5)
                    .build());
            assertTrue(response.getAccepted(), "submit rejected: " + response.getMessage());
        }
        for (String[] c : cases) {
            TaskStatusResponse status = waitForTerminal(c[0]);
            assertEquals(TaskStatus.COMPLETED, status.getStatus());
            assertEquals("worker-1", status.getWorkerId());
            assertTrue(status.getResult().contains("fake-executed"),
                    "unexpected result: " + status.getResult());
        }
    }

    @Test
    public void duplicateIdIsRejected() {
        String id = "dup-" + UUID.randomUUID();
        TaskResponse first = client.submitTask(com.predisched.proto.TaskRequest.newBuilder()
                .setTaskId(id)
                .setType(TaskType.CPU_TASK)
                .setInput("n=50")
                .setPriority(5)
                .build());
        assertTrue(first.getAccepted());
        TaskResponse second = client.submitTask(com.predisched.proto.TaskRequest.newBuilder()
                .setTaskId(id)
                .setType(TaskType.CPU_TASK)
                .setInput("n=50")
                .setPriority(5)
                .build());
        assertFalse(second.getAccepted());
        assertTrue(second.getMessage().contains("already exists"));
    }

    @Test
    public void cancelQueuedTaskSucceeds() throws Exception {
        // Separate service with no dispatcher running, so the task stays QUEUED.
        TaskStore store = new InMemoryTaskStore();
        TaskValidator validator = new TaskValidator(4096);
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        SchedulerServiceImpl service = new SchedulerServiceImpl(store, validator, queue);
        String name = "sched-cancel-" + UUID.randomUUID();
        Server server = InProcessServerBuilder.forName(name)
                .addService(service).directExecutor().build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            SchedulerServiceGrpc.SchedulerServiceBlockingStub stub =
                    SchedulerServiceGrpc.newBlockingStub(channel);
            String id = "cancel-" + UUID.randomUUID();
            TaskResponse submitted = stub.submitTask(com.predisched.proto.TaskRequest.newBuilder()
                    .setTaskId(id)
                    .setType(TaskType.SLEEP_TASK)
                    .setInput("ms=5")
                    .setPriority(5)
                    .build());
            assertTrue(submitted.getAccepted());
            TaskResponse cancelled = stub.cancelTask(
                    TaskStatusRequest.newBuilder().setTaskId(id).build());
            assertTrue(cancelled.getAccepted(), "cancel rejected: " + cancelled.getMessage());
            TaskStatusResponse status = stub.getTaskStatus(
                    TaskStatusRequest.newBuilder().setTaskId(id).build());
            assertEquals(TaskStatus.CANCELLED, status.getStatus());
            // Cancelling again must fail: only QUEUED tasks can be cancelled.
            TaskResponse again = stub.cancelTask(
                    TaskStatusRequest.newBuilder().setTaskId(id).build());
            assertFalse(again.getAccepted());
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }
}
