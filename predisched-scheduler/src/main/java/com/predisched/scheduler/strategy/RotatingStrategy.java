package com.predisched.scheduler.strategy;

import com.predisched.common.model.TaskRecord;
import com.predisched.common.model.WorkerInfo;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Temporary rotation over the alive list sorted by id, so worker churn does not
 * skip or repeat workers. Replaced by the real strategies in Prompt 03.
 */
public class RotatingStrategy implements SchedulingStrategy {

  private final AtomicInteger cursor = new AtomicInteger();

  @Override
  public String name() {
    return "rotating";
  }

  @Override
  public Optional<WorkerInfo> select(TaskRecord task, List<WorkerInfo> alive) {
    if (alive.isEmpty()) {
      return Optional.empty();
    }
    List<WorkerInfo> sorted =
        alive.stream().sorted(Comparator.comparing(WorkerInfo::workerId)).toList();
    return Optional.of(sorted.get(Math.floorMod(cursor.getAndIncrement(), sorted.size())));
  }
}
