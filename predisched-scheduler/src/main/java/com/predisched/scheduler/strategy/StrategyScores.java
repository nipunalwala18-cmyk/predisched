package com.predisched.scheduler.strategy;

import com.predisched.scheduler.WorkerInfo;
import java.util.List;
import java.util.Map;

/** Shared tie rule for scoring strategies: lowest score, then lowest worker id. */
final class StrategyScores {

    private StrategyScores() {}

    static WorkerInfo lowest(List<WorkerInfo> candidates, Map<String, Double> scores) {
        WorkerInfo best = null;
        for (WorkerInfo worker : candidates) {
            if (best == null) {
                best = worker;
                continue;
            }
            int byScore = Double.compare(scores.get(worker.id()), scores.get(best.id()));
            if (byScore < 0 || (byScore == 0 && worker.id().compareTo(best.id()) < 0)) {
                best = worker;
            }
        }
        return best;
    }
}
