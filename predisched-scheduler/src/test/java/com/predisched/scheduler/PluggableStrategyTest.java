package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
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
import com.predisched.scheduler.strategy.SchedulingDecision;
import com.predisched.scheduler.strategy.SchedulingStrategy;
import com.predisched.scheduler.strategy.StrategyRegistry;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A strategy defined only in this test, registered under a new name, drives real dispatches:
 * nothing outside its own class and one registry entry had to change.
 */
class PluggableStrategyTest {

    /** Always the worker whose id sorts last. */
    static final class LastIdStrategy implements SchedulingStrategy {
        @Override
        public String name() {
            return "last_id";
        }

        @Override
        public WorkerInfo select(TaskRecord task, List<WorkerInfo> candidates) {
            return candidates.stream()
                    .max((a, b) -> a.id().compareTo(b.id()))
                    .orElseThrow();
        }
    }

    /** Records which worker executed which task. */
    static final class RecordingWorker extends WorkerServiceGrpc.WorkerServiceImplBase {
        final String id;
        final Map<String, String> executed;

        RecordingWorker(String id, Map<String, String> executed) {
            this.id = id;
            this.executed = executed;
        }

        @Override
        public void executeTask(ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
            executed.put(request.getTask().getTaskId(), id);
            observer.onNext(ExecuteResult.newBuilder()
                    .setTaskId(request.getTask().getTaskId())
                    .setSuccess(true)
                    .setOutput("ok")
                    .setExecTimeMs(1L)
                    .build());
            observer.onCompleted();
        }
    }

    private final List<Server> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final Map<String, WorkerServiceGrpc.WorkerServiceBlockingStub> stubs =
            new ConcurrentHashMap<>();
    private final Map<String, String> executed = new ConcurrentHashMap<>();
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

    private void addWorker(String id) throws Exception {
        String name = "strategy-worker-" + UUID.randomUUID();
        servers.add(InProcessServerBuilder.forName(name)
                .addService(new RecordingWorker(id, executed)).build().start());
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        channels.add(channel);
        stubs.put(id, WorkerServiceGrpc.newBlockingStub(channel));
        registry.register(RegisterRequest.newBuilder()
                .setWorkerId(id).setHost("in-process").setPort(0)
                .setCores(4).setMemoryMb(512).setPoolSize(4).build());
    }

    private Dispatcher dispatcher(SchedulingStrategy strategy) {
        return new Dispatcher(store, queue, registry, worker -> stubs.get(worker.id()), retries,
                new RunningTasks(), 0L, 2.0, 2, 20, strategy);
    }

    @Test
    void aNewStrategyNeedsOnlyItsClassAndARegistryEntry() throws Exception {
        addWorker("worker-a");
        addWorker("worker-b");
        SchedulingStrategy strategy = StrategyRegistry.standard()
                .register("last_id", settings -> new LastIdStrategy())
                .create("last_id", StrategyRegistry.Settings.defaults());
        dispatcher = dispatcher(strategy);
        dispatcher.start();
        for (int i = 0; i < 6; i++) {
            TaskRecord task = TaskRecord.createQueued("p" + i, TaskType.SLEEP_TASK, "ms=1", 5);
            store.put(task);
            queue.add(task.id(), task.priority(), task.submittedAt());
        }
        awaitCompleted(6);

        assertEquals(6, executed.size());
        assertTrue(executed.values().stream().allMatch("worker-b"::equals), executed.toString());
        List<SchedulingDecision> decisions = dispatcher.decisions().recent();
        assertEquals(6, decisions.size());
        assertTrue(decisions.stream().allMatch(d -> d.strategy().equals("last_id")
                && d.workerId().equals("worker-b")));
    }

    @Test
    void aRetryAfterATimeoutOrRejectionAvoidsThatWorker() throws Exception {
        addWorker("worker-a");
        addWorker("worker-b");
        dispatcher = dispatcher(new LastIdStrategy());
        TaskRecord timedOut = TaskRecord.createQueued("r1", TaskType.SLEEP_TASK, "ms=1", 5)
                .withAttempt(new TaskAttempt(1, "worker-b", TaskAttempt.Outcome.TIMED_OUT,
                        "TIMEOUT", 0, 10, 10));
        TaskRecord failed = TaskRecord.createQueued("r2", TaskType.SLEEP_TASK, "ms=1", 5)
                .withAttempt(new TaskAttempt(1, "worker-b", TaskAttempt.Outcome.FAILED,
                        "bad input", 0, 10, 10));

        assertEquals("worker-a", dispatcher.chooseWorker(timedOut).orElseThrow().id(),
                "a timed-out task goes elsewhere");
        assertEquals("worker-b", dispatcher.chooseWorker(failed).orElseThrow().id(),
                "an ordinary failure is not the worker's fault; the strategy decides");
    }

    private void awaitCompleted(int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            long done = store.list().stream()
                    .filter(r -> r.status() == TaskStatus.COMPLETED).count();
            if (done >= expected) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("only some tasks completed: " + store.list());
    }
}
