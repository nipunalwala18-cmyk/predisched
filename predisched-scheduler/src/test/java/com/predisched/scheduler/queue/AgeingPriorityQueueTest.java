package com.predisched.scheduler.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** The ageing rule (F2, FR23), driven by an injected clock so nothing sleeps. */
class AgeingPriorityQueueTest {

    @Test
    void higherPriorityGoesFirst() throws Exception {
        AtomicLong now = new AtomicLong(10_000);
        AgeingPriorityQueue queue = new AgeingPriorityQueue(0.1, 10, now::get);
        queue.add("low", 1, now.get());
        queue.add("high", 9, now.get());
        queue.add("middle", 5, now.get());

        assertEquals("high", queue.take());
        assertEquals("middle", queue.take());
        assertEquals("low", queue.take());
    }

    @Test
    void equalPrioritiesStayFirstInFirstOut() throws Exception {
        AtomicLong now = new AtomicLong(10_000);
        AgeingPriorityQueue queue = new AgeingPriorityQueue(0.1, 10, now::get);
        queue.add("first", 5, 1_000);
        queue.add("second", 5, 2_000);
        queue.add("third", 5, 3_000);

        assertEquals("first", queue.take());
        assertEquals("second", queue.take());
        assertEquals("third", queue.take());
    }

    @Test
    void aWaitingLowPriorityTaskOvertakesFreshHighPriorityOnes() throws Exception {
        AtomicLong now = new AtomicLong(0);
        // 0.1 priority per second: a priority-1 task needs 40 s to pass a priority-5 arrival.
        AgeingPriorityQueue queue = new AgeingPriorityQueue(0.1, 10, now::get);
        queue.add("starving", 1, 0);

        now.set(10_000);
        queue.add("fresh-a", 5, now.get());
        assertEquals("fresh-a", queue.take(), "after 10 s the old task is only at 2.0");

        now.set(45_000);
        queue.add("fresh-b", 5, now.get());
        assertEquals("starving", queue.take(),
                "after 45 s the priority-1 task has aged to 5.5 and goes first");
    }

    @Test
    void ageingIsCappedAtTheMaximumPriority() {
        AtomicLong now = new AtomicLong(0);
        AgeingPriorityQueue queue = new AgeingPriorityQueue(1.0, 10, now::get);
        AgeingPriorityQueue.Entry entry = new AgeingPriorityQueue.Entry("t", 5, 0);

        assertEquals(6.0, queue.effectivePriority(entry, 1_000));
        assertEquals(10.0, queue.effectivePriority(entry, 60_000), "capped, not unbounded");
    }

    @Test
    void removeTakesATaskOutOfTheQueue() throws Exception {
        AtomicLong now = new AtomicLong(0);
        AgeingPriorityQueue queue = new AgeingPriorityQueue(0.1, 10, now::get);
        queue.add("a", 5, 0);
        queue.add("b", 5, 1);

        assertTrue(queue.remove("a"));
        assertFalse(queue.remove("missing"));
        assertEquals(1, queue.size());
        assertEquals("b", queue.take());
    }

    @Test
    void snapshotShowsTheOrderTasksWouldBeTaken() {
        AtomicLong now = new AtomicLong(60_000);
        AgeingPriorityQueue queue = new AgeingPriorityQueue(0.1, 10, now::get);
        queue.add("old-low", 1, 0);
        queue.add("new-high", 8, 60_000);
        queue.add("new-low", 2, 60_000);

        assertEquals(List.of("new-high", "old-low", "new-low"), queue.snapshot());
    }

    @Test
    void concurrentProducersAndConsumersLoseNothing() throws Exception {
        AgeingPriorityQueue queue = new AgeingPriorityQueue(0.1, 10);
        int producers = 4;
        int perProducer = 250;
        int total = producers * perProducer;
        ExecutorService pool = Executors.newFixedThreadPool(producers + 2);
        CountDownLatch taken = new CountDownLatch(total);

        for (int c = 0; c < 2; c++) {
            pool.execute(() -> {
                try {
                    while (true) {
                        queue.take();
                        taken.countDown();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        for (int p = 0; p < producers; p++) {
            final int producer = p;
            pool.execute(() -> {
                for (int i = 0; i < perProducer; i++) {
                    queue.add("p" + producer + "-t" + i, (i % 10) + 1, System.currentTimeMillis());
                }
            });
        }

        assertTrue(taken.await(30, TimeUnit.SECONDS),
                "every queued task was taken exactly once, " + taken.getCount() + " missing");
        pool.shutdownNow();
        assertEquals(0, queue.size());
    }
}
