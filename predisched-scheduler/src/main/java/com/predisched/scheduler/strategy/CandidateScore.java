package com.predisched.scheduler.strategy;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * One candidate's line in a decision breakdown (F13). The predictive strategy fills every field;
 * the reactive ones only {@code score} and {@code chosen}, with the prediction fields NaN.
 */
public record CandidateScore(
        String workerId,
        double score,
        boolean chosen,
        double predQueueLen,
        double predictedWaitMs,
        double predictedExecMs,
        double overloadProb,
        double overloadPenaltyMs,
        double cost,
        boolean skipped,
        String skipReason,
        boolean coldStart) {

    /** A reactive strategy's line: its own score, nothing predicted. */
    public static CandidateScore scoreOnly(String workerId, double score, boolean chosen) {
        return new CandidateScore(workerId, score, chosen, Double.NaN, Double.NaN, Double.NaN,
                Double.NaN, Double.NaN, Double.NaN, false, "", false);
    }

    public CandidateScore asChosen() {
        return new CandidateScore(workerId, score, true, predQueueLen, predictedWaitMs,
                predictedExecMs, overloadProb, overloadPenaltyMs, cost, skipped, skipReason,
                coldStart);
    }

    /** The breakdown as a JSON array for {@code scheduling_decisions.breakdown}. */
    public static String toJson(List<CandidateScore> breakdown) {
        return breakdown.stream().map(CandidateScore::json)
                .collect(Collectors.joining(",", "[", "]"));
    }

    private String json() {
        return String.format(Locale.ROOT, "{\"worker\":\"%s\",\"score\":%s,\"chosen\":%b,"
                        + "\"pred_queue_len\":%s,\"predicted_wait_ms\":%s,\"predicted_exec_ms\":%s,"
                        + "\"overload_prob\":%s,\"overload_penalty_ms\":%s,\"cost\":%s,"
                        + "\"skipped\":%b,\"skip_reason\":\"%s\",\"cold_start\":%b}",
                escape(workerId), num(score), chosen, num(predQueueLen), num(predictedWaitMs),
                num(predictedExecMs), num(overloadProb), num(overloadPenaltyMs), num(cost),
                skipped, escape(skipReason), coldStart);
    }

    private static String num(double value) {
        return Double.isFinite(value) ? String.format(Locale.ROOT, "%.6g", value) : "null";
    }

    private static String escape(String text) {
        return text == null ? "" : text.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
