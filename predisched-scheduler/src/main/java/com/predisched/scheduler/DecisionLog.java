package com.predisched.scheduler;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/** Bounded in-memory ring of recent {@link SchedulingDecision}s (default 1,000). */
public class DecisionLog {

  private final Deque<SchedulingDecision> ring = new ArrayDeque<>();
  private final int capacity;

  public DecisionLog(int capacity) {
    this.capacity = capacity;
  }

  public synchronized void add(SchedulingDecision decision) {
    if (ring.size() >= capacity) {
      ring.removeFirst();
    }
    ring.addLast(decision);
  }

  public synchronized List<SchedulingDecision> snapshot() {
    return List.copyOf(ring);
  }

  public synchronized int size() {
    return ring.size();
  }
}
