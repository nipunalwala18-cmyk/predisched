package com.predisched.scheduler.strategy;

import com.predisched.common.model.TaskRecord;
import com.predisched.common.model.WorkerInfo;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cycles over the alive list sorted by id. Sorting per call means worker churn
 * cannot skip or repeat a worker: the cursor is always modulo the current list.
 */
public class RoundRobinStrategy implements SchedulingStrategy {

  private final AtomicInteger cursor = new AtomicInteger();

  @Override
  public String name() {
    return "round-robin";
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
