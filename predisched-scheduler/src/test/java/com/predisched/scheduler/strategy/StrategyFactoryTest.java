package com.predisched.scheduler.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class StrategyFactoryTest {

  private final StrategyFactory factory = new StrategyFactory(42, 0.4, 0.2, 0.4);

  @Test
  void createsAllStrategies() {
    assertEquals("round-robin", factory.create("round-robin").name());
    assertEquals("random", factory.create("random").name());
    assertEquals("least-loaded", factory.create("least-loaded").name());
    assertEquals("resource-aware", factory.create("resource-aware").name());
  }

  @Test
  void namesAreCaseInsensitive() {
    assertEquals("random", factory.create("Random").name());
  }

  @Test
  void unknownNameFailsWithValidOptions() {
    var e = assertThrows(IllegalArgumentException.class, () -> factory.create("magic"));
    assertTrue(e.getMessage().contains("round-robin"));
    assertTrue(e.getMessage().contains("magic"));
  }
}
