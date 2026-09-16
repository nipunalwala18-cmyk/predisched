package com.predisched.scheduler.strategy;

import com.predisched.common.model.TaskRecord;
import com.predisched.common.model.WorkerInfo;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Picks the lowest weighted score:
 * {@code wCpu·cpuPct/100 + wMem·memPct/100 + wQueue·(queueLen+activeThreads)/poolSize}.
 *
 * <p>Each term is clamped to 0–1 (unavailable readings count as 0 load, queue load
 * saturates at one full pool), so the score is normalised to 0–1 for weights that
 * sum to 1. Ties go to the lowest worker id. Like {@link LeastLoadedStrategy}, this
 * sees the dispatcher's in-flight count folded into {@code queueLen}.
 */
public class ResourceAwareStrategy implements SchedulingStrategy {

  private final double wCpu;
  private final double wMem;
  private final double wQueue;

  public ResourceAwareStrategy(double wCpu, double wMem, double wQueue) {
    this.wCpu = wCpu;
    this.wMem = wMem;
    this.wQueue = wQueue;
  }

  @Override
  public String name() {
    return "resource-aware";
  }

  @Override
  public Optional<WorkerInfo> select(TaskRecord task, List<WorkerInfo> alive) {
    return alive.stream()
        .min(Comparator.comparingDouble(this::score).thenComparing(WorkerInfo::workerId));
  }

  double score(WorkerInfo w) {
    double cpu = clamp01(w.cpuPct() / 100.0);
    double mem = clamp01(w.memPct() / 100.0);
    double queue =
        w.poolSize() <= 0
            ? 1.0
            : Math.min(1.0, (double) (w.queueLen() + w.activeThreads()) / w.poolSize());
    return wCpu * cpu + wMem * mem + wQueue * queue;
  }

  private static double clamp01(double value) {
    if (Double.isNaN(value) || value < 0) {
      return 0;
    }
    return Math.min(1.0, value);
  }
}
