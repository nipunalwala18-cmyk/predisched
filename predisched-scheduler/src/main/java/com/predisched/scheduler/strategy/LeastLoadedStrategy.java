package com.predisched.scheduler.strategy;

import com.predisched.common.TaskRecord;
import com.predisched.scheduler.WorkerInfo;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The candidate with the lowest {@code queue_len + active_threads}; ties go to the lowest id.
 * Counts tasks, not their size, and ignores pool size: two tasks on an 8-thread worker count the
 * same as two on a 2-thread one.
 */
public class LeastLoadedStrategy implements SchedulingStrategy {

    @Override
    public String name() {
        return "least_loaded";
    }

    @Override
    public WorkerInfo select(TaskRecord task, List<WorkerInfo> candidates) {
        return StrategyScores.lowest(candidates, scores(task, candidates));
    }

    @Override
    public Map<String, Double> scores(TaskRecord task, List<WorkerInfo> candidates) {
        Map<String, Double> scores = new LinkedHashMap<>();
        for (WorkerInfo worker : candidates) {
            scores.put(worker.id(), (double) worker.load());
        }
        return scores;
    }
}
