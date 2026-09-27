package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.client.ExplainFormat;
import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.proto.DecisionExplanation;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerPrediction;
import com.predisched.proto.WorkerServiceGrpc;
import com.predisched.proto.WorkerState;
import com.predisched.scheduler.prediction.PredictionClient;
import com.predisched.scheduler.queue.AgeingPriorityQueue;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RetryPolicy;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.queue.TaskQueue;
import com.predisched.scheduler.strategy.PredictiveStrategy;
import com.predisched.scheduler.strategy.SchedulingDecision;
import com.predisched.scheduler.strategy.StrategyRegistry;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The predictive strategy inside a real dispatcher: decisions are timed and logged with their
 * breakdown, {@code explain} shows every candidate, the admin switches strategies, completions
 * feed the live MAE, and a failing predictor falls back without stopping dispatch.
 */
class PredictiveDispatchTest {

    /** Answers after sleeping ms=... so executions have a known length. */
    static final class SleepyWorker extends WorkerServiceGrpc.WorkerServiceImplBase {
        final String id;

        SleepyWorker(String id) {
            this.id = id;
        }

        @Override
        public void executeTask(ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
            observer.onNext(ExecuteResult.newBuilder()
                    .setTaskId(request.getTask().getTaskId())
                    .setSuccess(true).setOutput("ok").setExecTimeMs(25L).build());
            observer.onCompleted();
        }
    }

    private final List<Server> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final Map<String, WorkerServiceGrpc.WorkerServiceBlockingStub> stubs =
            new ConcurrentHashMap<>();
    private final TaskStore store = new InMemoryTaskStore();
    private final TaskQueue queue = new AgeingPriorityQueue(0.1, 10);
    private final RetryCoordinator retries = new RetryCoordinator(
            store, queue, new RetryPolicy(10, 100, 0, 3, 1), new DeadLetterQueue());
    private final WorkerRegistry registry = new WorkerRegistry(60_000);
    private Dispatcher dispatcher;

    @AfterEach
    void stop() {
        if (dispatcher != null) {
            dispatcher.close();
        }
        retries.close();
        channels.forEach(ManagedChannel::shutdownNow);
        servers.forEach(Server::shutdownNow);
    }

    private void addWorker(String id, int pool, double slowdown) throws Exception {
        String name = "predictive-worker-" + UUID.randomUUID();
        servers.add(InProcessServerBuilder.forName(name).addService(new SleepyWorker(id))
                .build().start());
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        channels.add(channel);
        stubs.put(id, WorkerServiceGrpc.newBlockingStub(channel));
        registry.register(RegisterRequest.newBuilder().setWorkerId(id).setHost("in-process")
                .setPort(0).setCores(8).setMemoryMb(512).setPoolSize(pool).setSlowdown(slowdown)
                .build());
    }

    /** Predicts 20 ms everywhere but 10 ms on worker-b; fails on demand. */
    static final class FakePredictor implements PredictiveStrategy.Predictor {
        final AtomicBoolean down = new AtomicBoolean();
        final List<List<WorkerState>> requests = new ArrayList<>();

        @Override
        public synchronized PredictionClient.Result predict(com.predisched.proto.TaskRequest task,
                java.util.Collection<WorkerState> workers, double typeMeanMs, String requestId) {
            if (down.get()) {
                return new PredictionClient.Result(Map.of(), 0, "", 10.0, 0,
                        "DEADLINE_EXCEEDED: deadline exceeded after 10ms");
            }
            requests.add(List.copyOf(workers));
            Map<String, WorkerPrediction> byWorker = new LinkedHashMap<>();
            for (WorkerState w : workers) {
                byWorker.put(w.getWorkerId(), WorkerPrediction.newBuilder()
                        .setWorkerId(w.getWorkerId())
                        .setPredExecMs(w.getWorkerId().equals("worker-b") ? 10.0 : 20.0)
                        .setOverloadProb(0.1).build());
            }
            return new PredictionClient.Result(byWorker, 1, "m1=v1,m2=v1,m3=v1", 1.0, 0.8, null);
        }
    }

    private void submit(String id) {
        TaskRecord task = TaskRecord.createQueued(id, TaskType.SLEEP_TASK, "ms=25", 5);
        store.put(task);
        queue.add(task.id(), task.priority(), task.submittedAt());
    }

    private void awaitCompleted(int n) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (store.list().stream().filter(t -> t.status() == TaskStatus.COMPLETED).count() < n) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("tasks did not complete: " + store.list());
            }
            Thread.sleep(10);
        }
    }

    @Test
    void decisionsAreTimedExplainedAndFallBackWhenPredictionsStop() throws Exception {
        addWorker("worker-a", 2, 2.0);
        addWorker("worker-b", 4, 1.0);
        addWorker("worker-c", 8, 1.0);
        FakePredictor predictor = new FakePredictor();
        PredictiveStrategy strategy = new PredictiveStrategy(predictor,
                PredictiveStrategy.Settings.defaults());
        dispatcher = new Dispatcher(store, queue, registry, worker -> stubs.get(worker.id()),
                retries, new RunningTasks(), 0L, 2.0, 2, 20, strategy);
        dispatcher.start();

        submit("p1");
        awaitCompleted(1);
        SchedulingDecision first = dispatcher.decisions().find("p1");
        assertEquals("predictive", first.strategy());
        assertEquals("worker-b", first.workerId());
        assertFalse(first.fallback());
        assertTrue(first.decisionMicros() > 0, "decision time recorded");
        assertEquals(3, first.breakdown().size());
        // The worker's registered slowdown reaches the prediction request.
        assertEquals(2.0, predictor.requests.get(0).stream()
                .filter(w -> w.getWorkerId().equals("worker-a")).findFirst().orElseThrow()
                .getSlowdown());

        DispatcherAdmin admin = new DispatcherAdmin(dispatcher, StrategyRegistry.standard(),
                StrategyRegistry.Settings.defaults());
        DecisionExplanation explained = admin.explain("p1");
        String table = ExplainFormat.format(explained);
        for (String worker : List.of("worker-a", "worker-b", "worker-c")) {
            assertTrue(table.contains(worker), table);
        }
        assertTrue(table.contains("* worker-b"), table);
        assertTrue(table.contains("m1=v1,m2=v1,m3=v1"), table);

        // The live MAE: worker-b was predicted 10 ms and reported 25 ms.
        long deadline = System.currentTimeMillis() + 5_000;
        while (strategy.accuracy().tasks() < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(15.0, strategy.accuracy().mae(), 1e-9);

        // Predictions stop: least loaded places the next task, marked as a fallback.
        predictor.down.set(true);
        submit("p2");
        awaitCompleted(2);
        SchedulingDecision second = dispatcher.decisions().find("p2");
        assertTrue(second.fallback());
        assertTrue(second.fallbackReason().startsWith("least_loaded: DEADLINE_EXCEEDED"),
                second.fallbackReason());
        String fallbackTable = ExplainFormat.format(admin.explain("p2"));
        assertTrue(fallbackTable.contains("fallback: least_loaded: DEADLINE_EXCEEDED"),
                fallbackTable);

        // Switch strategy through the admin, as `workload replay --strategy` does.
        assertTrue(admin.setStrategy("round_robin").getOk());
        assertEquals("round_robin", dispatcher.strategy().name());
        assertFalse(admin.setStrategy("fastest").getOk());
        assertFalse(admin.explain("never-submitted").getFound());
    }
}
