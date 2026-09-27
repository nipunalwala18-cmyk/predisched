package com.predisched.scheduler.strategy;

import com.predisched.common.TaskRecord;
import com.predisched.scheduler.WorkerInfo;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleSupplier;

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

    /**
     * The whole decision: worker, scores and the per-candidate breakdown (F13). The dispatcher
     * calls this and times it. The default is {@link #select} plus {@link #scores}; the
     * predictive strategy overrides it to make one prediction call per task.
     */
    default StrategyDecision decide(TaskRecord task, List<WorkerInfo> candidates) {
        WorkerInfo chosen = select(task, candidates);
        return StrategyDecision.of(chosen, scores(task, candidates), candidates);
    }

    /** What the dispatcher offers a strategy beyond the candidates (prompt 18). */
    record Context(DoubleSupplier arrivalRatePerSecond) {
        public static final Context NONE = new Context(() -> 0.0);
    }

    /** Called when the strategy is installed in a dispatcher. */
    default void attach(Context context) {}

    /** The predicted execution time of a task this strategy placed, if it made one (F11). */
    default java.util.OptionalDouble predictedExecMs(String taskId) {
        return java.util.OptionalDouble.empty();
    }

    /** An attempt this strategy placed has ended (prompt 18: accuracy tracking). */
    default void completed(TaskRecord task, String workerId, long execMs, boolean success) {}
}
