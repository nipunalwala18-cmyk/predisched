package com.predisched.common.clock;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * In-memory ring of {@link Event}s (persisted to PostgreSQL from Prompt 08).
 * Thread-safe via its own monitor.
 */
public class EventLog {

  private final Deque<Event> ring = new ArrayDeque<>();
  private final int capacity;

  public EventLog() {
    this(5000);
  }

  public EventLog(int capacity) {
    this.capacity = capacity;
  }

  public synchronized void record(Event event) {
    if (ring.size() >= capacity) {
      ring.removeFirst();
    }
    ring.addLast(event);
  }

  public synchronized List<Event> snapshot() {
    return new ArrayList<>(ring);
  }

  /** Events for one task, merged and sorted by (Lamport time, node id). */
  public synchronized List<Event> forTask(String taskId) {
    return ring.stream()
        .filter(e -> e.taskId().equals(taskId))
        .sorted(Event.ORDER)
        .toList();
  }

  public synchronized int size() {
    return ring.size();
  }
}
