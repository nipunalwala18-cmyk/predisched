package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.db.Db;
import com.predisched.common.db.HistoryWriter;
import com.predisched.common.db.TestDatabase;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerServiceGrpc;
import com.predisched.scheduler.cache.ResultCache;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** F6: identical deterministic work is served from the cache; the rest always runs. */
class ResultCacheTest {

    /** Echoes its input; only SLEEP_TASK is non-deterministic, as with the real executors. */
    private static final class EchoWorker extends WorkerServiceGrpc.WorkerServiceImplBase {
        final Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();

        @Override
        public void executeTask(ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
            TaskRequest task = request.getTask();
            runs.computeIfAbsent(task.getType() + " " + task.getInput(),
                    key -> new AtomicInteger()).incrementAndGet();
            observer.onNext(ExecuteResult.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setSuccess(true)
                    .setOutput("result of " + task.getInput())
                    .setExecTimeMs(5)
                    .setCacheable(task.getType() != TaskType.SLEEP_TASK)
                    .build());
            observer.onCompleted();
        }

        int runsOf(TaskType type, String input) {
            AtomicInteger count = runs.get(type + " " + input);
            return count == null ? 0 : count.get();
        }
    }

    @Test
    void theKeyIgnoresParameterOrderAndSpacing() {
        assertEquals(ResultCache.key(TaskType.MONTE_CARLO_TASK, "samples=10, seed=3"),
                ResultCache.key(TaskType.MONTE_CARLO_TASK, "seed=3,samples=10"));
        assertNotEquals(ResultCache.key(TaskType.MONTE_CARLO_TASK, "samples=10, seed=3"),
                ResultCache.key(TaskType.MONTE_CARLO_TASK, "samples=10, seed=4"));
        assertNotEquals(ResultCache.key(TaskType.CPU_TASK, "n=10"),
                ResultCache.key(TaskType.SORT_TASK, "n=10"));
    }

    @Test
    void aSecondIdenticalCpuTaskIsAHitSleepIsNeverCachedAndOtherInputMisses() throws Exception {
        EchoWorker worker = new EchoWorker();
        try (SchedulerHarness harness = new SchedulerHarness(worker)) {
            ResultCache cache = new ResultCache(100, null,
                    com.predisched.common.db.HistorySink.NONE);
            harness.service.useResultCache(cache);
            harness.dispatcher.useResultCache(cache);
            harness.start();

            complete(harness, "c1", TaskType.CPU_TASK, "n=5000", false);
            TaskStatusResponse hit = complete(harness, "c2", TaskType.CPU_TASK, "n=5000", false);
            assertEquals("cache", hit.getWorkerId());
            assertEquals("result of n=5000", hit.getResult());
            assertEquals(1, worker.runsOf(TaskType.CPU_TASK, "n=5000"), "ran once, then cached");

            complete(harness, "c3", TaskType.CPU_TASK, "n=6000", false);
            assertEquals(1, worker.runsOf(TaskType.CPU_TASK, "n=6000"), "different input ran");

            complete(harness, "s1", TaskType.SLEEP_TASK, "ms=1", false);
            TaskStatusResponse sleep = complete(harness, "s2", TaskType.SLEEP_TASK, "ms=1", false);
            assertEquals("worker-1", sleep.getWorkerId());
            assertEquals(2, worker.runsOf(TaskType.SLEEP_TASK, "ms=1"), "SLEEP_TASK never cached");

            TaskStatusResponse bypass = complete(harness, "c4", TaskType.CPU_TASK, "n=5000", true);
            assertEquals("worker-1", bypass.getWorkerId(), "--no-cache runs it");
            assertEquals(1, cache.hits());
        }
    }

    @Test
    void aDeadlineIsRecordedAndMetWhenTheTaskFinishesInTime() throws Exception {
        try (SchedulerHarness harness = new SchedulerHarness(new EchoWorker())) {
            harness.start();
            TaskStatusResponse met = complete(harness, "d1", TaskType.CPU_TASK, "n=10",
                    false, 60_000);
            assertTrue(met.getDeadlineAt() > 0);
            assertTrue(met.getSlaMet());
            TaskStatusResponse none = complete(harness, "d2", TaskType.CPU_TASK, "n=11",
                    false, 0);
            assertEquals(0, none.getDeadlineAt());
            assertFalse(none.getSlaMet());
        }
    }

    @Test
    void aResultStoredInTheDatabaseIsFoundByANewScheduler() throws Exception {
        try (TestDatabase database = TestDatabase.create(); Db db = database.open(2, true)) {
            try (HistoryWriter writer = new HistoryWriter(db.dataSource(), 100, 10, 20)) {
                writer.start();
                ResultCache first = new ResultCache(100, db.dataSource(), writer);
                first.store(TaskType.HASH_TASK, "rounds=7", "abc");
                assertTrue(writer.flush(10_000));

                // A fresh cache, as after a restart: empty memory, the table still has it.
                ResultCache second = new ResultCache(100, db.dataSource(), writer);
                assertEquals("abc", second.lookup(TaskType.HASH_TASK, "rounds=7").orElseThrow());
                assertTrue(second.lookup(TaskType.HASH_TASK, "rounds=8").isEmpty());
                assertTrue(writer.flush(10_000));
            }
            try (var connection = db.dataSource().getConnection();
                    var statement = connection.createStatement();
                    var result = statement.executeQuery("SELECT hits FROM result_cache")) {
                result.next();
                assertEquals(1, result.getLong(1), "the hit was counted in the table");
            }
        }
    }

    private static TaskStatusResponse complete(SchedulerHarness harness, String id,
            TaskType type, String input, boolean noCache) throws InterruptedException {
        return complete(harness, id, type, input, noCache, 0);
    }

    private static TaskStatusResponse complete(SchedulerHarness harness, String id,
            TaskType type, String input, boolean noCache, long deadlineMs)
            throws InterruptedException {
        assertTrue(harness.client.submitTask(TaskRequest.newBuilder()
                .setTaskId(id).setType(type).setInput(input).setPriority(5)
                .setNoCache(noCache).setDeadlineMs(deadlineMs).build()).getAccepted());
        long deadline = System.currentTimeMillis() + 5_000;
        TaskStatusResponse status;
        do {
            Thread.sleep(5);
            status = harness.client.getTaskStatus(
                    TaskStatusRequest.newBuilder().setTaskId(id).build());
        } while (status.getStatus() != TaskStatus.COMPLETED
                && System.currentTimeMillis() < deadline);
        assertEquals(TaskStatus.COMPLETED, status.getStatus(), id);
        return status;
    }
}
