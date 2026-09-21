package com.predisched.common;

/** Outcome of running one task executor locally on a worker. */
public record ExecutionResult(boolean success, String output, String errorMessage) {

    public static ExecutionResult success(String output) {
        return new ExecutionResult(true, output == null ? "" : output, "");
    }

    public static ExecutionResult failure(String errorMessage) {
        return new ExecutionResult(false, "", errorMessage == null ? "" : errorMessage);
    }
}
