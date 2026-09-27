package com.predisched.scheduler.strategy;

import java.util.List;
import java.util.Map;

/**
 * One worker choice: which task, which strategy, which worker, what each candidate scored (empty
 * for strategies that do not score) and how long the choice took. Since prompt 18 it also keeps
 * the per-candidate breakdown (F13), and whether the predictive strategy fell back and why.
 */
public record SchedulingDecision(
        String taskId, String strategy, String workerId, Map<String, Double> scores,
        long decisionMicros, long atMs, List<CandidateScore> breakdown, boolean fallback,
        String fallbackReason, String modelVersions) {

    public SchedulingDecision(String taskId, String strategy, String workerId,
            Map<String, Double> scores, long decisionMicros, long atMs) {
        this(taskId, strategy, workerId, scores, decisionMicros, atMs, List.of(), false, "", "");
    }
}
