package com.predisched.scheduler.strategy;

import com.predisched.scheduler.WorkerInfo;
import java.util.List;
import java.util.Map;

/**
 * What a strategy returns for one task (prompt 18): the worker, the per-candidate scores, the
 * breakdown behind them (F13), and whether the strategy had to fall back and why.
 */
public record StrategyDecision(
        WorkerInfo chosen,
        Map<String, Double> scores,
        List<CandidateScore> breakdown,
        boolean fallback,
        String fallbackReason,
        String modelVersions) {

    /** A reactive decision: scores only, the breakdown built from them. */
    public static StrategyDecision of(WorkerInfo chosen, Map<String, Double> scores,
            List<WorkerInfo> candidates) {
        return new StrategyDecision(chosen, scores, breakdownOf(chosen, scores, candidates),
                false, "", "");
    }

    static List<CandidateScore> breakdownOf(WorkerInfo chosen, Map<String, Double> scores,
            List<WorkerInfo> candidates) {
        return candidates.stream()
                .map(w -> CandidateScore.scoreOnly(w.id(),
                        scores.getOrDefault(w.id(), Double.NaN), w.id().equals(chosen.id())))
                .toList();
    }
}
