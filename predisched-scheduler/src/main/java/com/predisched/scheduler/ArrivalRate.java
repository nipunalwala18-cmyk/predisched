package com.predisched.scheduler;

import com.predisched.common.clock.NodeClock;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Tasks per second over a sliding 10-second window (an ML feature later).
 *
 * <p>Thread-safe via its own monitor. Only accepted submissions are marked.
 */
public class ArrivalRate {

  /** Window length in milliseconds. */
  static final long WINDOW_MS = 10_000;

  private final Deque<Long> arrivals = new ArrayDeque<>();
  private final NodeClock wall;

  public ArrivalRate(NodeClock wall) {
    this.wall = wall;
  }

  /** Record one arrival at the current time. */
  public synchronized void mark() {
    long now = wall.now();
    arrivals.addLast(now);
    evict(now);
  }

  /** Tasks per second over the window ending now. */
  public synchronized double ratePerSecond() {
    evict(wall.now());
    return (double) arrivals.size() / (WINDOW_MS / 1000.0);
  }

  private void evict(long now) {
    while (!arrivals.isEmpty() && now - arrivals.getFirst() > WINDOW_MS) {
      arrivals.removeFirst();
    }
  }
}
