package com.predisched.scheduler.strategy;

import static com.predisched.scheduler.strategy.StrategyTestSupport.task;
import static com.predisched.scheduler.strategy.StrategyTestSupport.worker;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class LeastLoadedStrategyTest {

  @Test
  void picksLowestQueuePlusThreads() {
    var strategy = new LeastLoadedStrategy();
    var alive =
        List.of(
            worker("w-a", 2, 1, 0, 0, 4), // load 3
            worker("w-b", 0, 2, 0, 0, 4), // load 2
            worker("w-c", 1, 0, 0, 0, 4)); // load 1
    assertEquals("w-c", strategy.select(task(), alive).orElseThrow().workerId());
  }

  @Test
  void tiesBreakById() {
    var strategy = new LeastLoadedStrategy();
    var alive =
        List.of(
            worker("w-b", 0, 1, 0, 0, 4), // load 1
            worker("w-a", 1, 0, 0, 0, 4)); // load 1
    assertEquals("w-a", strategy.select(task(), alive).orElseThrow().workerId());
  }

  @Test
  void emptyListSelectsNothing() {
    assertTrue(new LeastLoadedStrategy().select(task(), List.of()).isEmpty());
  }
}
