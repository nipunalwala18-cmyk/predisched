package com.predisched.common.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LamportClockTest {

    @Test
    void tickAdvancesByOne() {
        LamportClock clock = new LamportClock();
        assertEquals(0, clock.current());
        assertEquals(1, clock.tick());
        assertEquals(2, clock.tick());
        assertEquals(2, clock.current());
    }

    @Test
    void updateTakesTheMaximumPlusOne() {
        LamportClock clock = new LamportClock(5);
        assertEquals(11, clock.update(10), "received is ahead: max(5,10)+1");
        assertEquals(12, clock.update(3), "received is behind: max(11,3)+1");
        assertEquals(13, clock.update(12), "equal: max(12,12)+1");
    }

    @Test
    void concurrentTicksNeverRepeatAValue() throws Exception {
        LamportClock clock = new LamportClock();
        int threads = 8;
        int perThread = 2000;
        Set<Long> seen = ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            pool.execute(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    seen.add(clock.tick());
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        assertEquals(threads * perThread, seen.size(), "every tick handed out a distinct value");
        assertEquals(threads * perThread, clock.current());
    }

    @Test
    void concurrentUpdatesAlwaysMoveForward() throws Exception {
        LamportClock clock = new LamportClock();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        for (int t = 0; t < 4; t++) {
            final int base = t * 100;
            pool.execute(() -> {
                for (int i = 0; i < 500; i++) {
                    long before = clock.current();
                    long after = clock.update(base + i);
                    assertTrue(after > before, "the clock never goes backwards");
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
    }

    @Test
    void physicalClockAppliesOffsetDriftAndCorrections() {
        long[] system = {1_000_000L};
        PhysicalClock clock = new PhysicalClock(-300, 0, () -> system[0]);
        assertEquals(999_700L, clock.now(), "offset applied");

        system[0] += 10_000L;
        assertEquals(1_009_700L, clock.now(), "no drift configured");

        clock.adjust(300);
        assertEquals(1_010_000L, clock.now(), "correction removes the offset");
        assertEquals(300, clock.totalCorrectionMs());

        long[] driftingSystem = {0L};
        PhysicalClock drifting = new PhysicalClock(0, 1_000_000, () -> driftingSystem[0]);
        driftingSystem[0] = 10_000L;
        assertEquals(20_000L, drifting.now(), "1e6 ppm doubles the elapsed time");
    }
}
