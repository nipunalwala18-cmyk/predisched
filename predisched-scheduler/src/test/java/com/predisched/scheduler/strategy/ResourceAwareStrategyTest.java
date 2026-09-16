package com.predisched.scheduler.strategy;

import static com.predisched.scheduler.strategy.StrategyTestSupport.task;
import static com.predisched.scheduler.strategy.StrategyTestSupport.worker;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class ResourceAwareStrategyTest {

  private static ResourceAwareStrategy strategy() {
    return new ResourceAwareStrategy(0.4, 0.2, 0.4);
  }

  @Test
  void picksLowestWeightedScore() {
    // w-a: 0.4*0.1 + 0.2*0.2 + 0.4*0   = 0.08
    // w-b: 0.4*0.8 + 0.2*0.7 + 0.4*0   = 0.46
    // w-c: 0.4*0.1 + 0.2*0.2 + 0.4*1.0 = 0.48 (queue 3 + active 1 saturates pool 4)
    var alive =
        List.of(
            worker("w-b", 0, 0, 80, 70, 4),
            worker("w-c", 3, 1, 10, 20, 4),
            worker("w-a", 0, 0, 10, 20, 4));
    assertEquals("w-a", strategy().select(task(), alive).orElseThrow().workerId());
  }

  @Test
  void tiesBreakById() {
    var alive = List.of(worker("w-b", 0, 0, 10, 10, 4), worker("w-a", 0, 0, 10, 10, 4));
    assertEquals("w-a", strategy().select(task(), alive).orElseThrow().workerId());
  }

  @Test
  void unavailableReadingsCountAsZeroLoad() {
    // cpu/mem -1 (bean unavailable) must not poison the score.
    var alive =
        List.of(
            worker("w-a", 0, 0, -1, -1, 4), // score 0
            worker("w-b", 0, 0, 50, 50, 4)); // score 0.3
    assertEquals("w-a", strategy().select(task(), alive).orElseThrow().workerId());
  }

  @Test
  void emptyListSelectsNothing() {
    assertTrue(strategy().select(task(), List.of()).isEmpty());
  }
}
