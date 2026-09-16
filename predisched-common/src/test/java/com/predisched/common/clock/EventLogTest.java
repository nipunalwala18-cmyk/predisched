package com.predisched.common.clock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class EventLogTest {

  @Test
  void forTaskReturnsLamportOrderedEvents() {
    EventLog log = new EventLog();
    log.record(new Event("sched", 3, 100, EventType.DISPATCH, "t1", "worker=w1"));
    log.record(new Event("sched", 1, 90, EventType.SUBMIT, "t1", ""));
    log.record(new Event("w1", 2, 95, EventType.START, "t1", ""));
    log.record(new Event("sched", 4, 110, EventType.SUBMIT, "other", ""));
    List<Event> events = log.forTask("t1");
    assertEquals(3, events.size());
    assertEquals(1, events.get(0).lamport());
    assertEquals(2, events.get(1).lamport());
    assertEquals(3, events.get(2).lamport());
  }

  @Test
  void ringIsBounded() {
    EventLog log = new EventLog(10);
    for (int i = 0; i < 25; i++) {
      log.record(new Event("n", i, i, EventType.SUBMIT, "t", ""));
    }
    assertEquals(10, log.size());
    assertEquals(15, log.snapshot().get(0).lamport());
  }

  @Test
  void nodeContextEmitTicks() {
    NodeContext ctx =
        new NodeContext("n1", new LamportClock(), new NodeClock(0, 0), new EventLog());
    ctx.emit(EventType.SUBMIT, "t", "");
    ctx.emit(EventType.ENQUEUE, "t", "");
    assertEquals(2, ctx.events().size());
    assertEquals(2, ctx.lamport().current());
    assertTrue(ctx.events().forTask("t").get(0).wallTimeMs() > 0);
  }
}
