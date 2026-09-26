package com.predisched.scheduler.strategy;

import java.util.Map;

/**
 * One worker choice: which task, which strategy, which worker, what each candidate scored (empty
 * for strategies that do not score) and how long the choice took.
 */
public record SchedulingDecision(
        String taskId, String strategy, String workerId, Map<String, Double> scores,
        long decisionMicros, long atMs) {}
