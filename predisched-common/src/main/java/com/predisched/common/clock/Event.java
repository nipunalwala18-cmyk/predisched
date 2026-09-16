package com.predisched.common.clock;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

/** A cluster event with Lamport and wall-clock timestamps. */
public record Event(
    String nodeId, long lamport, long wallTimeMs, EventType type, String taskId, String details) {

  /** Chronological for display: Lamport order, ties by node id. */
  public static final Comparator<Event> ORDER =
      Comparator.comparingLong(Event::lamport).thenComparing(Event::nodeId);
}
