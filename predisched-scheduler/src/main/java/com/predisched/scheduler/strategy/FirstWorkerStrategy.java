package com.predisched.scheduler.strategy;

import com.predisched.common.model.TaskRecord;
import com.predisched.common.model.WorkerInfo;
import java.util.List;
import java.util.Optional;

/** Placeholder used until Prompt 03: always picks the first worker. */
public class FirstWorkerStrategy implements SchedulingStrategy {

  @Override
  public String name() {
    return "first-worker";
  }

  @Override
  public Optional<WorkerInfo> select(TaskRecord task, List<WorkerInfo> alive) {
    if (alive.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(alive.get(0));
  }
}
