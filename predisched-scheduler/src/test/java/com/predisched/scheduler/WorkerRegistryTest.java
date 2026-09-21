package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.proto.Heartbeat;
import com.predisched.proto.RegisterRequest;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class WorkerRegistryTest {

    private static RegisterRequest registration(String id, int port, int poolSize) {
        return RegisterRequest.newBuilder()
                .setWorkerId(id)
                .setHost("localhost")
                .setPort(port)
                .setCores(8)
                .setMemoryMb(2048)
                .setPoolSize(poolSize)
                .build();
    }

    private static Heartbeat heartbeat(String id, int active, int queued) {
        return Heartbeat.newBuilder()
                .setWorkerId(id)
                .setCpuPct(42.0)
                .setMemPct(10.0)
                .setActiveThreads(active)
                .setQueueLen(queued)
                .setTasksCompleted(7)
                .setAvgExecMs(12.5)
                .build();
    }

    @Test
    void registrationThenHeartbeatUpdatesMetricsInPlace() {
        WorkerRegistry registry = new WorkerRegistry(5000);
        registry.register(registration("worker-1", 51061, 4));
        registry.heartbeat(heartbeat("worker-1", 3, 5));

        WorkerInfo worker = registry.get("worker-1").orElseThrow();
        assertEquals(4, worker.poolSize(), "registration facts survive a heartbeat");
        assertEquals(3, worker.activeThreads());
        assertEquals(8, worker.load());
        assertEquals(7, worker.tasksCompleted());
        assertEquals(1, registry.size());
    }

    @Test
    void heartbeatFromAnUnknownWorkerIsRefused() {
        WorkerRegistry registry = new WorkerRegistry(5000);
        assertTrue(registry.heartbeat(heartbeat("ghost", 0, 0)).isEmpty());
        assertEquals(0, registry.size());
    }

    @Test
    void silentWorkersAreNotHealthy() {
        AtomicLong now = new AtomicLong(1_000);
        WorkerRegistry registry = new WorkerRegistry(5_000, now::get);
        registry.register(registration("worker-1", 51061, 4));

        assertEquals(1, registry.healthy().size());
        now.addAndGet(6_000);
        assertTrue(registry.healthy().isEmpty(), "no heartbeat for 6 s with a 5 s limit");
        assertEquals(1, registry.all().size(), "still registered, just not dispatchable");

        registry.heartbeat(heartbeat("worker-1", 0, 0));
        assertEquals(1, registry.healthy().size(), "a heartbeat brings it back");
    }

    @Test
    void concurrentHeartbeatsLeaveOneConsistentEntryPerWorker() throws Exception {
        WorkerRegistry registry = new WorkerRegistry(60_000);
        for (int w = 0; w < 3; w++) {
            registry.register(registration("worker-" + w, 51060 + w, 4));
        }
        int threads = 12;
        int perThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            final int worker = t % 3;
            pool.execute(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    registry.heartbeat(heartbeat("worker-" + worker, i % 4, i % 7));
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        assertEquals(3, registry.size());
        List<WorkerInfo> all = registry.all();
        assertEquals(List.of("worker-0", "worker-1", "worker-2"),
                all.stream().map(WorkerInfo::id).toList());
        for (WorkerInfo worker : all) {
            assertEquals(4, worker.poolSize());
            assertFalse(worker.activeThreads() < 0);
        }
    }
}
