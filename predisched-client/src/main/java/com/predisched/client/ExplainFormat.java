package com.predisched.client;

import com.predisched.proto.CandidateBreakdown;
import com.predisched.proto.DecisionExplanation;
import java.time.Instant;
import java.util.Locale;

/** {@code predisched explain}: a decision's per-worker breakdown as a table (F13, prompt 18). */
public final class ExplainFormat {

    private ExplainFormat() {}

    public static String format(DecisionExplanation e) {
        StringBuilder out = new StringBuilder();
        if (!e.getFound()) {
            return out.append(e.getMessage()).append('\n').toString();
        }
        out.append(String.format(Locale.ROOT, "task %s: strategy %s chose %s in %.2f ms at %s%n",
                e.getTaskId(), e.getStrategy(), e.getChosenWorker(), e.getDecisionUs() / 1000.0,
                Instant.ofEpochMilli(e.getAtMs())));
        if (e.getFallback()) {
            out.append("fallback: ").append(e.getFallbackReason()).append('\n');
        }
        if (!e.getModelVersions().isEmpty()) {
            out.append("models: ").append(e.getModelVersions()).append('\n');
        }
        boolean predicted = e.getCandidatesList().stream()
                .anyMatch(c -> Double.isFinite(c.getPredictedExecMs()));
        if (predicted) {
            out.append(String.format(Locale.ROOT, "  %-12s %9s %11s %11s %9s %11s %11s  %s%n",
                    "worker", "pred_queue", "pred_wait", "pred_exec", "overload", "penalty",
                    "cost", "note"));
            for (CandidateBreakdown c : e.getCandidatesList()) {
                String note = c.getSkipped() ? "skipped: " + c.getSkipReason() : "";
                if (c.getColdStart()) {
                    note = (note.isEmpty() ? "" : note + "; ") + "cold start";
                }
                out.append(String.format(Locale.ROOT,
                        "%s %-12s %9.2f %9.1f ms %8.1f ms %9.3f %8.1f ms %8.1f ms  %s%n",
                        c.getChosen() ? "*" : " ", c.getWorkerId(), c.getPredQueueLen(),
                        c.getPredictedWaitMs(), c.getPredictedExecMs(), c.getOverloadProb(),
                        c.getOverloadPenaltyMs(), c.getCost(), note).stripTrailing())
                        .append('\n');
            }
            out.append("cost = (pred_wait + pred_exec) x (1 + lambda x overload); lowest wins"
                    + " (* = chosen)\n");
        } else {
            out.append(String.format(Locale.ROOT, "  %-12s %9s%n", "worker", "score"));
            for (CandidateBreakdown c : e.getCandidatesList()) {
                out.append(String.format(Locale.ROOT, "%s %-12s %9.3f%n",
                        c.getChosen() ? "*" : " ", c.getWorkerId(), c.getScore()));
            }
            out.append("score: the strategy's own number, lower is better (* = chosen)\n");
        }
        return out.toString();
    }
}
