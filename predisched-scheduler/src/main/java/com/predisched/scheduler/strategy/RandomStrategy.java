package com.predisched.scheduler.strategy;

import com.predisched.common.model.TaskRecord;
import com.predisched.common.model.WorkerInfo;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;

/** Uniform random pick. Seeded from config, so runs are reproducible. */
public class RandomStrategy implements SchedulingStrategy {

  private final SplittableRandom random;

  public RandomStrategy(long seed) {
    this.random = new SplittableRandom(seed);
  }

  @Override
  public String name() {
    return "random";
  }

  @Override
  public Optional<WorkerInfo> select(TaskRecord task, List<WorkerInfo> alive) {
    if (alive.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(alive.get(random.nextInt(alive.size())));
  }
}
