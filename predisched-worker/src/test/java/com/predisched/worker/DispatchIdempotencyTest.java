package com.predisched.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.ExecutionState;
import com.predisched.proto.ExecutionStatus;
import com.predisched.proto.QueryExecutionRequest;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** A worker runs each dispatch id at most once (prompt 10). */
class DispatchIdempotencyTest {

    @Test
    void aSecondCallWithTheSameDispatchIdWaitsForTheFirstRunInsteadOfStartingAnother()
            throws Exception {
        WorkerMetrics metrics = new WorkerMetrics();
        try (ExecutionEngine engine =
                new ExecutionEngine(new ExecutorRegistry(), metrics, "w", 2, 10)) {
            CompletableFuture<ExecutionEngine.Outcome> first =
                    engine.submit("t", "t#1", TaskType.SLEEP_TASK, "ms=300", "");
            assertEquals(ExecutionEngine.DispatchState.RUNNING, engine.query("t#1"));
            CompletableFuture<ExecutionEngine.Outcome> again =
                    engine.submit("t", "t#1", TaskType.SLEEP_TASK, "ms=300", "");
            assertSame(first, again, "re-attached to the running dispatch");

            assertTrue(again.get(10, TimeUnit.SECONDS).success());
            assertEquals(ExecutionEngine.DispatchState.FINISHED, engine.query("t#1"));

            // After it finished: the recorded outcome, still one run.
            ExecutionEngine.Outcome late =
                    engine.submit("t", "t#1", TaskType.SLEEP_TASK, "ms=300", "")
                            .get(1, TimeUnit.SECONDS);
            assertEquals(first.get().output(), late.output());
            assertEquals(1, metrics.tasksCompleted(), "the attempt ran once");

            // A new attempt of the same task is a new dispatch and runs.
            assertTrue(engine.submit("t", "t#2", TaskType.SLEEP_TASK, "ms=10", "")
                    .get(10, TimeUnit.SECONDS).success());
            assertEquals(2, metrics.tasksCompleted());
            assertEquals(ExecutionEngine.DispatchState.UNKNOWN, engine.query("t#9"));
        }
    }

    @Test
    void queryExecutionReportsRunningFinishedWithItsResultAndUnknown() throws Exception {
        String name = "worker-" + UUID.randomUUID();
        try (ExecutionEngine engine = new ExecutionEngine(
                new ExecutorRegistry(), new WorkerMetrics(), "w", 2, 10)) {
            Server server = InProcessServerBuilder.forName(name)
                    .addService(new WorkerServiceImpl(engine, "w")).build().start();
            ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
            try {
                WorkerServiceGrpc.WorkerServiceBlockingStub stub =
                        WorkerServiceGrpc.newBlockingStub(channel);
                WorkerServiceGrpc.WorkerServiceFutureStub async =
                        WorkerServiceGrpc.newFutureStub(channel);
                var reply = async.executeTask(ExecuteRequest.newBuilder()
                        .setTask(TaskRequest.newBuilder().setTaskId("q")
                                .setType(TaskType.SLEEP_TASK).setInput("ms=300"))
                        .setDispatchId("q#1")
                        .build());
                waitFor(() -> engine.query("q#1") == ExecutionEngine.DispatchState.RUNNING);
                assertEquals(ExecutionState.EXECUTION_RUNNING, query(stub, "q#1").getState());

                ExecuteResult result = reply.get(10, TimeUnit.SECONDS);
                ExecutionStatus done = query(stub, "q#1");
                assertEquals(ExecutionState.EXECUTION_FINISHED, done.getState());
                assertEquals(result.getOutput(), done.getResult().getOutput());
                assertTrue(done.getResult().getSuccess());

                assertEquals(ExecutionState.EXECUTION_UNKNOWN, query(stub, "never#1").getState());
            } finally {
                channel.shutdownNow();
                server.shutdownNow();
            }
        }
    }

    private static ExecutionStatus query(WorkerServiceGrpc.WorkerServiceBlockingStub stub,
            String dispatchId) {
        return stub.queryExecution(QueryExecutionRequest.newBuilder()
                .setTaskId(dispatchId.substring(0, dispatchId.indexOf('#')))
                .setDispatchId(dispatchId)
                .build());
    }

    private static void waitFor(java.util.function.BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(condition.getAsBoolean(), "condition not reached");
    }
}
