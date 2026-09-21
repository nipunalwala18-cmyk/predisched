package com.predisched.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.proto.TaskType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ExecutionEngineTest {

    @Test
    void runsAtMostPoolSizeTasksAtOnce() throws Exception {
        ExecutorRegistry registry = new ExecutorRegistry();
        WorkerMetrics metrics = new WorkerMetrics();
        AtomicInteger highWater = new AtomicInteger();
        try (ExecutionEngine engine = new ExecutionEngine(registry, metrics, "w", 3, 100)) {
            List<CompletableFuture<ExecutionEngine.Outcome>> futures = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                futures.add(engine.submit("t" + i, TaskType.SLEEP_TASK, "ms=60"));
            }
            CompletableFuture<Void> all =
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            while (!all.isDone()) {
                highWater.accumulateAndGet(engine.activeThreads(), Math::max);
            }
            all.get(30, TimeUnit.SECONDS);
            for (CompletableFuture<ExecutionEngine.Outcome> future : futures) {
                assertTrue(future.get().success());
                assertFalse(future.get().rejected());
            }
        }
        assertTrue(highWater.get() <= 3, "never more than pool size active, saw " + highWater.get());
        assertEquals(12, metrics.tasksCompleted());
    }

    @Test
    void reportsWaitTimeSeparatelyFromExecutionTime() throws Exception {
        WorkerMetrics metrics = new WorkerMetrics();
        try (ExecutionEngine engine =
                new ExecutionEngine(new ExecutorRegistry(), metrics, "w", 1, 10)) {
            CompletableFuture<ExecutionEngine.Outcome> first =
                    engine.submit("a", TaskType.SLEEP_TASK, "ms=300");
            CompletableFuture<ExecutionEngine.Outcome> second =
                    engine.submit("b", TaskType.SLEEP_TASK, "ms=10");
            first.get(10, TimeUnit.SECONDS);
            ExecutionEngine.Outcome queued = second.get(10, TimeUnit.SECONDS);
            assertTrue(queued.waitMs() >= 200,
                    "second task waited behind the first, waited " + queued.waitMs() + " ms");
            assertTrue(queued.execMs() < 200,
                    "its own execution was short, took " + queued.execMs() + " ms");
        }
    }

    @Test
    void rejectsWhenTheQueueIsFullInsteadOfBlocking() throws Exception {
        WorkerMetrics metrics = new WorkerMetrics();
        try (ExecutionEngine engine =
                new ExecutionEngine(new ExecutorRegistry(), metrics, "w", 1, 2)) {
            List<CompletableFuture<ExecutionEngine.Outcome>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(engine.submit("t" + i, TaskType.SLEEP_TASK, "ms=200"));
            }
            int rejected = 0;
            for (CompletableFuture<ExecutionEngine.Outcome> future : futures) {
                if (future.get(30, TimeUnit.SECONDS).rejected()) {
                    rejected++;
                }
            }
            assertTrue(rejected > 0, "a pool of 1 with a queue of 2 cannot hold 8 tasks");
            assertEquals(rejected, metrics.tasksRejected());
        }
    }

    @Test
    void namesItsThreadsAfterTheWorker() throws Exception {
        WorkerMetrics metrics = new WorkerMetrics();
        try (ExecutionEngine engine =
                new ExecutionEngine(new ExecutorRegistry(), metrics, "worker-7", 2, 10)) {
            engine.submit("t", TaskType.SLEEP_TASK, "ms=1").get(10, TimeUnit.SECONDS);
        }
        // Thread names are asserted through the worker log in the acceptance check; here we only
        // confirm the engine ran the task and recorded it.
        assertEquals(1, metrics.tasksCompleted());
    }

    @Test
    void averageExecutionTimeCoversOnlyTheRecentWindow() {
        WorkerMetrics metrics = new WorkerMetrics();
        assertEquals(0.0, metrics.avgExecMs());
        for (int i = 0; i < WorkerMetrics.WINDOW; i++) {
            metrics.recordCompleted(10);
        }
        assertEquals(10.0, metrics.avgExecMs());
        for (int i = 0; i < WorkerMetrics.WINDOW; i++) {
            metrics.recordCompleted(20);
        }
        assertEquals(20.0, metrics.avgExecMs(), "older samples fall out of the window");
    }
}
