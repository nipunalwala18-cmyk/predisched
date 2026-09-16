package com.predisched.scheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scheduler-local per-worker count of tasks dispatched but not yet resolved.
 *
 * <p>Heartbeat snapshots can be up to a second old, so between two heartbeats the
 * scheduler would otherwise see a worker it just loaded as still idle and pile on
 * more tasks. The dispatcher increments on dispatch and decrements on the result,
 * and folds the count into the load the strategies see.
 */
public class InFlight {

  private final ConcurrentHashMap<String, AtomicInteger> counts = new ConcurrentHashMap<>();

  public void increment(String workerId) {
    counts.computeIfAbsent(workerId, id -> new AtomicInteger()).incrementAndGet();
  }

  public void decrement(String workerId) {
    counts.computeIfAbsent(workerId, id -> new AtomicInteger()).decrementAndGet();
  }

  public int get(String workerId) {
    AtomicInteger count = counts.get(workerId);
    return count == null ? 0 : Math.max(0, count.get());
  }
}
