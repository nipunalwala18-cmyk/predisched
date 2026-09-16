package com.predisched.scheduler.strategy;

import com.predisched.common.model.TaskRecord;
import com.predisched.common.model.WorkerInfo;
import java.util.List;
import java.util.Optional;

/**
 * Pluggable placement policy (spec §9 Exp 6).
 *
 * <p>A new strategy is one class implementing this interface, selected by config.
 */
public interface SchedulingStrategy {

  String name();

  Optional<WorkerInfo> select(TaskRecord task, List<WorkerInfo> alive);

  /** Called after a task completes; used by the predictive strategy later. */
  default void onOutcome(TaskRecord task, WorkerInfo worker, long execMs) {}
}
