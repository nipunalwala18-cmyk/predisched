package com.predisched.scheduler.strategy;

import java.util.List;
import java.util.Locale;

/**
 * Creates strategies from {@code settings.strategy}. Unknown names fail fast with
 * the list of valid names, so a misconfigured scheduler refuses to start instead
 * of silently running the wrong policy.
 */
public class StrategyFactory {

  public static final List<String> NAMES =
      List.of("round-robin", "random", "least-loaded", "resource-aware");

  private final long seed;
  private final double wCpu;
  private final double wMem;
  private final double wQueue;

  public StrategyFactory(long seed, double wCpu, double wMem, double wQueue) {
    this.seed = seed;
    this.wCpu = wCpu;
    this.wMem = wMem;
    this.wQueue = wQueue;
  }

  public SchedulingStrategy create(String name) {
    if (name == null) {
      throw new IllegalArgumentException("strategy name is null, expected one of " + NAMES);
    }
    return switch (name.toLowerCase(Locale.ROOT).trim()) {
      case "round-robin" -> new RoundRobinStrategy();
      case "random" -> new RandomStrategy(seed);
      case "least-loaded" -> new LeastLoadedStrategy();
      case "resource-aware" -> new ResourceAwareStrategy(wCpu, wMem, wQueue);
      default ->
          throw new IllegalArgumentException(
              "unknown strategy: " + name + ", expected one of " + NAMES);
    };
  }
}
