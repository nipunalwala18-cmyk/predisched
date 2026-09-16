package com.predisched.scheduler;

/**
 * One recorded placement: which strategy chose which worker from how many
 * candidates, and how long the choice took. Kept in a bounded in-memory ring
 * ({@link DecisionLog}); persisted from Prompt 08.
 */
public record SchedulingDecision(
    String taskId, String strategy, String workerId, int candidates, long decisionMicros) {}
