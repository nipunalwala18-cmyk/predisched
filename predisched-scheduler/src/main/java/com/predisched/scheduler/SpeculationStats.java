package com.predisched.scheduler;

import java.util.concurrent.atomic.AtomicLong;

/** Speculative execution counters (prompt 19, F11). */
public final class SpeculationStats {

    private final AtomicLong launched = new AtomicLong();
    private final AtomicLong duplicateWins = new AtomicLong();
    private final AtomicLong originalWins = new AtomicLong();
    private final AtomicLong wastedMs = new AtomicLong();

    void recordLaunch() {
        launched.incrementAndGet();
    }

    void recordDuplicateWin() {
        duplicateWins.incrementAndGet();
    }

    void recordOriginalWin() {
        originalWins.incrementAndGet();
    }

    /** Time the losing copy had run when it was cancelled. */
    void recordWasted(long ms) {
        wastedMs.addAndGet(Math.max(0, ms));
    }

    public long launched() {
        return launched.get();
    }

    public long duplicateWins() {
        return duplicateWins.get();
    }

    public long originalWins() {
        return originalWins.get();
    }

    public long wastedMs() {
        return wastedMs.get();
    }

    public String summary() {
        return "speculations launched=" + launched.get() + ", won by the copy="
                + duplicateWins.get() + ", won by the original=" + originalWins.get()
                + ", wasted work=" + wastedMs.get() + " ms";
    }
}
