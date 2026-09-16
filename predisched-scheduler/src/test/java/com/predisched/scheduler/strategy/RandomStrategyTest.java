package com.predisched.scheduler.strategy;

import static com.predisched.scheduler.strategy.StrategyTestSupport.pickIds;
import static com.predisched.scheduler.strategy.StrategyTestSupport.task;
import static com.predisched.scheduler.strategy.StrategyTestSupport.worker;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class RandomStrategyTest {

  @Test
  void sameSeedReproducesSequence() {
    var alive = List.of(worker("w-a"), worker("w-b"), worker("w-c"));
    assertEquals(pickIds(new RandomStrategy(42), alive, 20), pickIds(new RandomStrategy(42), alive, 20));
  }

  @Test
  void differentSeedsDiverge() {
    var alive = List.of(worker("w-a"), worker("w-b"), worker("w-c"), worker("w-d"));
    assertNotEquals(
        pickIds(new RandomStrategy(1), alive, 20), pickIds(new RandomStrategy(2), alive, 20));
  }

  @Test
  void singleWorkerAlwaysPicked() {
    var alive = List.of(worker("w-a"));
    assertEquals(List.of("w-a", "w-a", "w-a"), pickIds(new RandomStrategy(7), alive, 3));
  }

  @Test
  void emptyListSelectsNothing() {
    assertTrue(new RandomStrategy(7).select(task(), List.of()).isEmpty());
  }
}
