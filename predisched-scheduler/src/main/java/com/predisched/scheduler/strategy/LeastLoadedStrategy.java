package com.predisched.scheduler.strategy;

import com.predisched.common.model.TaskRecord;
import com.predisched.common.model.WorkerInfo;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Picks the lowest {@code queueLen + activeThreads}, ties broken by worker id.
 *
 * <p>The dispatcher folds its local in-flight count into the {@code queueLen} it
 * passes in, so decisions account for tasks dispatched-but-unacknowledged that
 * heartbeats (up to a second old) cannot show yet.
 */
public class LeastLoadedStrategy implements SchedulingStrategy {

  @Override
  public String name() {
    return "least-loaded";
  }

  @Override
  public Optional<WorkerInfo> select(TaskRecord task, List<WorkerInfo> alive) {
    return alive.stream()
        .min(
            Comparator.comparingInt((WorkerInfo w) -> w.queueLen() + w.activeThreads())
                .thenComparing(WorkerInfo::workerId));
  }
}
