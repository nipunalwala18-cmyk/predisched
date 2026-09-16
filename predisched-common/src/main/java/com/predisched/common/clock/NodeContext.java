package com.predisched.common.clock;

/**
 * Everything a node needs for trustworthy time: its id, its Lamport clock, its
 * (possibly skewed, synchronised) wall clock, and its event log.
 *
 * <p>Passed to services so no service code handles clocks by hand.
 */
public record NodeContext(String nodeId, LamportClock lamport, NodeClock wall, EventLog events) {

  /** Record an event, ticking the Lamport clock for this local event. */
  public void emit(EventType type, String taskId, String details) {
    events.record(new Event(nodeId, lamport.tick(), wall.now(), type, taskId, details));
  }
}
