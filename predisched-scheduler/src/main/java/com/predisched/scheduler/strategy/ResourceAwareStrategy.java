package com.predisched.scheduler.strategy;

import com.predisched.common.TaskRecord;
import com.predisched.scheduler.WorkerInfo;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A weighted score per candidate, lowest wins, ties to the lowest id:
 *
 * <pre>
 *   score = cpuWeight * cpu% / 100 + memWeight * mem% / 100 + queueWeight * load / poolSize
 * </pre>
 *
 * CPU and memory come from the worker's last heartbeat; load is queued plus running work, so
 * dividing by pool size makes a busy 8-thread worker look less full than a busy 2-thread one.
 * Weights come from config ({@code scheduling.weights}).
 */
public class ResourceAwareStrategy implements SchedulingStrategy {

    private final double cpuWeight;
    private final double memWeight;
    private final double queueWeight;

    public ResourceAwareStrategy(double cpuWeight, double memWeight, double queueWeight) {
        this.cpuWeight = cpuWeight;
        this.memWeight = memWeight;
        this.queueWeight = queueWeight;
    }

    @Override
    public String name() {
        return "resource_aware";
    }

    @Override
    public WorkerInfo select(TaskRecord task, List<WorkerInfo> candidates) {
        return StrategyScores.lowest(candidates, scores(task, candidates));
    }

    @Override
    public Map<String, Double> scores(TaskRecord task, List<WorkerInfo> candidates) {
        Map<String, Double> scores = new LinkedHashMap<>();
        for (WorkerInfo worker : candidates) {
            double queueShare = (double) worker.load() / Math.max(1, worker.poolSize());
            scores.put(worker.id(), cpuWeight * worker.cpuPct() / 100.0
                    + memWeight * worker.memPct() / 100.0
                    + queueWeight * queueShare);
        }
        return scores;
    }
}
