package com.predisched.scheduler.strategy;

import static com.predisched.scheduler.strategy.StrategyTestSupport.pickIds;
import static com.predisched.scheduler.strategy.StrategyTestSupport.task;
import static com.predisched.scheduler.strategy.StrategyTestSupport.worker;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class RoundRobinStrategyTest {

  @Test
  void visitsEveryWorkerOncePerCycle() {
    var strategy = new RoundRobinStrategy();
    // Deliberately unsorted: the strategy sorts by id itself.
    var alive = List.of(worker("w-c"), worker("w-a"), worker("w-b"));
    assertEquals(List.of("w-a", "w-b", "w-c", "w-a", "w-b", "w-c"), pickIds(strategy, alive, 6));
  }

  @Test
  void workerRemovedMidCycleStillCoversRemainder() {
    var strategy = new RoundRobinStrategy();
    var three = List.of(worker("w-a"), worker("w-b"), worker("w-c"));
    assertEquals(List.of("w-a", "w-b"), pickIds(strategy, three, 2));
    var two = List.of(worker("w-a"), worker("w-b"));
    // Cursor continues modulo the live list: no skip, no repeat, no crash.
    assertEquals(List.of("w-a", "w-b", "w-a", "w-b"), pickIds(strategy, two, 4));
  }

  @Test
  void emptyListSelectsNothing() {
    assertTrue(new RoundRobinStrategy().select(task(), List.of()).isEmpty());
  }

  @Test
  void nameIsRoundRobin() {
    assertEquals("round-robin", new RoundRobinStrategy().name());
  }
}
