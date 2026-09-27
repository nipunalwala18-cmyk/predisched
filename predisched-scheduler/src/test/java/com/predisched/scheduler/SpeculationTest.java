package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.NodeConfig;
import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.proto.Ack;
import com.predisched.proto.CancelRequest;
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
import com.predisched.scheduler.strategy.LeastLoadedStrategy;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Speculative execution (prompt 19, F11): the straggler threshold, and a long-tail workload with
 * one slow worker where duplicates win, losers are cancelled, and every task completes once.
 */
class SpeculationTest {

    /** Sleeps {@code ms × factor}; a cancel ends the sleep and the reply says so. */
    static final class FakeWorker extends WorkerServiceGrpc.WorkerServiceImplBase {
        final String id;
        final double factor;
        final Map<String, CountDownLatch> running = new ConcurrentHashMap<>();
        final AtomicInteger cancels = new AtomicInteger();
        final AtomicInteger executions = new AtomicInteger();

        FakeWorker(String id, double factor) {
            this.id = id;
            this.factor = factor;
        }

        @Override
        public void executeTask(ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
            String taskId = request.getTask().getTaskId();
            long ms = (long) (Long.parseLong(request.getTask().getInput().replace("ms=", ""))
                    * factor);
            CountDownLatch cancelled = new CountDownLatch(1);
            running.put(taskId, cancelled);
            executions.incrementAndGet();
            boolean wasCancelled;
            try {
                wasCancelled = cancelled.await(ms, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                wasCancelled = true;
            }
            running.remove(taskId);
            observer.onNext(ExecuteResult.newBuilder().setTaskId(taskId)
                    .setSuccess(!wasCancelled)
                    .setOutput(wasCancelled ? "error: cancelled" : "slept " + ms)
                    .setExecTimeMs(ms).build());
            observer.onCompleted();
        }

        @Override
        public void cancelExecution(CancelRequest request, StreamObserver<Ack> observer) {
            CountDownLatch latch = running.get(request.getTaskId());
            if (latch != null) {
                cancels.incrementAndGet();
                latch.countDown();
            }
            observer.onNext(Ack.newBuilder().setOk(latch != null).build());
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
    private final RunningTasks running = new RunningTasks();
    private Dispatcher dispatcher;
    private StragglerDetector detector;

    @AfterEach
    void stop() {
        if (detector != null) {
            detector.close();
        }
        if (dispatcher != null) {
            dispatcher.close();
        }
        retries.close();
        channels.forEach(ManagedChannel::shutdownNow);
        servers.forEach(Server::shutdownNow);
    }

    private FakeWorker addWorker(String id, int pool, double factor) throws Exception {
        FakeWorker worker = new FakeWorker(id, factor);
        String name = "spec-" + id + "-" + UUID.randomUUID();
        servers.add(InProcessServerBuilder.forName(name).addService(worker)
                .executor(java.util.concurrent.Executors.newCachedThreadPool()).build().start());
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        channels.add(channel);
        stubs.put(id, WorkerServiceGrpc.newBlockingStub(channel));
        registry.register(RegisterRequest.newBuilder().setWorkerId(id).setHost("in-process")
                .setPort(0).setCores(4).setMemoryMb(512).setPoolSize(pool).build());
        return worker;
    }

    @Test
    void thresholdIsKTimesThePredictionOrTheTypeP90WhicheverIsLarger() {
        OptionalDouble none = OptionalDouble.empty();
        assertEquals(200.0, StragglerDetector.threshold(2, OptionalDouble.of(100),
                OptionalDouble.of(150)));
        assertEquals(300.0, StragglerDetector.threshold(2, OptionalDouble.of(100),
                OptionalDouble.of(300)));
        assertEquals(150.0, StragglerDetector.threshold(2, none, OptionalDouble.of(150)),
                "no prediction: the p90 alone");
        assertEquals(200.0, StragglerDetector.threshold(2, OptionalDouble.of(100), none),
                "no history yet: k x prediction");
        assertTrue(Double.isNaN(StragglerDetector.threshold(2, none, none)));
    }

    @Test
    void typeP90NeedsEnoughSamplesAndKeepsTheWindow() {
        TypeExecTimes times = new TypeExecTimes(10);
        for (int i = 1; i <= 9; i++) {
            times.record("SLEEP_TASK", i * 10);
        }
        assertTrue(times.p90("SLEEP_TASK", 10).isEmpty());
        times.record("SLEEP_TASK", 100);
        assertEquals(90.0, times.p90("SLEEP_TASK", 10).getAsDouble());
        for (int i = 0; i < 10; i++) {
            times.record("SLEEP_TASK", 5);
        }
        assertEquals(5.0, times.p90("SLEEP_TASK", 10).getAsDouble(), "old runs left the window");
    }

    @Test
    void aSlowWorkersStragglersAreRacedAndEachTaskCompletesOnce() throws Exception {
        FakeWorker slow = addWorker("worker-1", 4, 25.0);   // 25x slower
        addWorker("worker-2", 4, 1.0);
        addWorker("worker-3", 4, 1.0);
        dispatcher = new Dispatcher(store, queue, registry, worker -> stubs.get(worker.id()),
                retries, running, 0L, 2.0, 8, 20, new LeastLoadedStrategy());
        NodeConfig.SpeculationConfig config = new NodeConfig.SpeculationConfig();
        config.setEnabled(true);
        config.setCheckMs(25);
        config.setK(2.0);
        config.setMinSamples(5);
        detector = new StragglerDetector(dispatcher, running, store, config);
        dispatcher.start();
        detector.start();

        // A long tail: mostly 20-60 ms, a few 150 ms.
        int n = 40;
        for (int i = 0; i < n; i++) {
            long ms = i % 10 == 9 ? 150 : 20 + (i * 7) % 40;
            TaskRecord task = TaskRecord.createQueued("lt-" + i, TaskType.SLEEP_TASK,
                    "ms=" + ms, 5);
            store.put(task);
            queue.add(task.id(), task.priority(), task.submittedAt());
            Thread.sleep(15);
        }
        long deadline = System.currentTimeMillis() + 30_000;
        while (store.list().stream().filter(t -> t.status() == TaskStatus.COMPLETED).count() < n) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("not all completed: " + store.list());
            }
            Thread.sleep(20);
        }

        SpeculationStats stats = dispatcher.speculation();
        assertTrue(stats.launched() >= 1, stats.summary());
        assertTrue(stats.duplicateWins() >= 1, stats.summary());
        assertTrue(slow.cancels.get() >= 1, "the slow worker's losing copies were cancelled");
        int losers = 0;
        for (TaskRecord task : store.list()) {
            assertEquals(TaskStatus.COMPLETED, task.status(), task.id());
            long succeeded = task.attempts().stream().filter(TaskAttempt::succeeded).count();
            assertEquals(1, succeeded, "completed exactly once: " + task.attempts());
            losers += (int) task.attempts().stream()
                    .filter(a -> a.outcome() == TaskAttempt.Outcome.SPECULATIVE_LOSER).count();
        }
        assertTrue(losers >= 1);
        assertTrue(stats.wastedMs() > 0);
        assertEquals(0, running.size(), "nothing left in flight");
    }

    @Test
    void aDrainingWorkerGetsNoNewDispatches() throws Exception {
        addWorker("worker-1", 4, 1.0);
        addWorker("worker-2", 4, 1.0);
        dispatcher = new Dispatcher(store, queue, registry, worker -> stubs.get(worker.id()),
                retries, running, 0L, 2.0, 2, 20, new LeastLoadedStrategy());
        TaskRecord probe = TaskRecord.createQueued("probe", TaskType.SLEEP_TASK, "ms=1", 5);
        assertEquals(2, dispatcher.candidates(probe).size());
        registry.heartbeat(Heartbeat.newBuilder().setWorkerId("worker-1").setDraining(true)
                .build());
        assertTrue(registry.isDraining("worker-1"));
        List<WorkerInfo> candidates = dispatcher.candidates(probe);
        assertEquals(1, candidates.size());
        assertEquals("worker-2", candidates.get(0).id());
        registry.heartbeat(Heartbeat.newBuilder().setWorkerId("worker-1").build());
        assertFalse(registry.isDraining("worker-1"), "a heartbeat without the flag clears it");
    }
}
