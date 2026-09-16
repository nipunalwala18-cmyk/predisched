package com.predisched.common.clock;

/** One node's outcome from a clock-sync round. */
public record ClockSyncRecord(
    String nodeId, long ts, long offsetBeforeMs, long offsetAfterMs, String algorithm) {}
