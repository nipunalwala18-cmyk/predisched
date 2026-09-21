package com.predisched.common;

/**
 * One try at running a task (F4). A task that is retried after a failure, a timeout or a lost
 * worker collects one of these per try, so `status` can show the whole history and the ML dataset
 * in prompt 15 can tell a first-time success from a third-attempt one.
 *
 * @param attempt      1 for the first try
 * @param workerId     where it ran, empty if it never reached a worker
 * @param outcome      how the attempt ended
 * @param reason       detail for a failure: the error, TIMEOUT, WORKER_LOST, REJECTED
 * @param startedAtMs  physical time the attempt was dispatched
 * @param endedAtMs    physical time the outcome was known
 * @param execTimeMs   time the worker spent executing, 0 when it never ran
 */
public record TaskAttempt(
        int attempt,
        String workerId,
        Outcome outcome,
        String reason,
        long startedAtMs,
        long endedAtMs,
        long execTimeMs) {

    /** How an attempt ended. Only SUCCEEDED is terminal for the task. */
    public enum Outcome {
        SUCCEEDED,
        FAILED,
        TIMED_OUT,
        REJECTED,
        WORKER_LOST
    }

    public boolean succeeded() {
        return outcome == Outcome.SUCCEEDED;
    }

    @Override
    public String toString() {
        return "#" + attempt + " " + outcome
                + (workerId.isEmpty() ? "" : " on " + workerId)
                + (reason.isEmpty() ? "" : " (" + reason + ")")
                + " " + execTimeMs + "ms";
    }
}
