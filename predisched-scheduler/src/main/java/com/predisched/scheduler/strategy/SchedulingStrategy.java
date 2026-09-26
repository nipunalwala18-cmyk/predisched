package com.predisched.scheduler.strategy;

import com.predisched.common.TaskRecord;
import com.predisched.scheduler.WorkerInfo;
import java.util.List;
import java.util.Map;

/**
 * Picks the worker a task goes to (FR11, Exp 6). A new strategy is one class implementing this
 * plus one entry in {@link StrategyRegistry}; the dispatcher never asks which strategy it has.
 *
 * <p>Candidates are healthy workers with spare capacity, never empty, their load already
 * including this scheduler's in-flight dispatches. Implementations are called from the single
 * dispatcher thread but must still be thread-safe: the strategy can be swapped at runtime.
 */
public interface SchedulingStrategy {

    /** The name used in config ({@code scheduling.strategy}) and in decision records. */
    String name();

    WorkerInfo select(TaskRecord task, List<WorkerInfo> candidates);

    /**
     * Per-candidate scores behind a choice, for the decision log; lower is better. Strategies
     * that do not score (round robin, random) return an empty map.
     */
    default Map<String, Double> scores(TaskRecord task, List<WorkerInfo> candidates) {
        return Map.of();
    }
}
