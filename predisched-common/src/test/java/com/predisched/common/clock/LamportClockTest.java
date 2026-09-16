package com.predisched.common.clock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LamportClockTest {

  @Test
  void tickIncrements() {
    LamportClock clock = new LamportClock();
    assertEquals(1, clock.tick());
    assertEquals(2, clock.tick());
    assertEquals(2, clock.current());
  }

  @Test
  void updateTakesMaxPlusOne() {
    LamportClock clock = new LamportClock();
    clock.tick(); // 1
    assertEquals(6, clock.update(5)); // max(1,5)+1
    assertEquals(7, clock.update(3)); // max(6,3)+1, stale sender does not rewind
    assertEquals(7, clock.current());
  }

  @Test
  void concurrentTicksNeverDuplicate() throws Exception {
    LamportClock clock = new LamportClock();
    int threads = 8;
    int perThread = 1000;
    Set<Long> seen = java.util.Collections.synchronizedSet(new HashSet<>());
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch done = new CountDownLatch(threads);
    for (int t = 0; t < threads; t++) {
      pool.execute(
          () -> {
            for (int i = 0; i < perThread; i++) {
              seen.add(clock.tick());
            }
            done.countDown();
          });
    }
    assertTrue(done.await(30, TimeUnit.SECONDS));
    pool.shutdownNow();
    assertEquals(threads * perThread, seen.size());
    assertEquals(threads * perThread, clock.current());
  }
}
