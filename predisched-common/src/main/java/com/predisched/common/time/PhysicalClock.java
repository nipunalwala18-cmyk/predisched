package com.predisched.common.time;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * A node's physical clock: the system clock plus an artificial offset and drift, so clock
 * synchronisation has something to correct on a single machine (lab Exp 3).
 *
 * <p>{@code driftPpm} is parts per million: 200 means this clock gains 200 microseconds per second
 * of wall time. Everything that needs "now" for ordering, timestamps or logging reads this, not
 * {@code System.currentTimeMillis()}.
 */
public class PhysicalClock {

    private final LongSupplier systemClock;
    private final long startedAtMs;
    private final double driftPpm;
    private final AtomicLong offsetMs;
    private final AtomicLong totalCorrectionMs = new AtomicLong();

    public PhysicalClock(long offsetMs, double driftPpm) {
        this(offsetMs, driftPpm, System::currentTimeMillis);
    }

    public PhysicalClock(long offsetMs, double driftPpm, LongSupplier systemClock) {
        this.systemClock = systemClock;
        this.startedAtMs = systemClock.getAsLong();
        this.driftPpm = driftPpm;
        this.offsetMs = new AtomicLong(offsetMs);
    }

    /** A clock with no offset and no drift, for tests and for nodes that do not simulate skew. */
    public static PhysicalClock system() {
        return new PhysicalClock(0L, 0.0);
    }

    public long now() {
        long system = systemClock.getAsLong();
        long drift = (long) ((system - startedAtMs) * driftPpm / 1_000_000.0);
        return system + offsetMs.get() + drift;
    }

    /** Applies a correction, as Berkeley and Cristian both do. Positive moves the clock forward. */
    public void adjust(long correctionMs) {
        offsetMs.addAndGet(correctionMs);
        totalCorrectionMs.addAndGet(correctionMs);
    }

    /** How far this clock currently sits from the underlying system clock, in ms. */
    public long offsetFromSystemMs() {
        return now() - systemClock.getAsLong();
    }

    public long totalCorrectionMs() {
        return totalCorrectionMs.get();
    }
}
