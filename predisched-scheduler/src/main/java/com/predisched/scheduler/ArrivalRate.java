package com.predisched.scheduler;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.LongSupplier;

/**
 * Accepted submits per second over the last {@code windowMs} (10 s), an ML feature (spec §12.1).
 * A lock-free deque of submit times, trimmed from the old end on every read and write.
 */
public class ArrivalRate {

    private final ConcurrentLinkedDeque<Long> arrivals = new ConcurrentLinkedDeque<>();
    private final long windowMs;
    private final LongSupplier clock;

    public ArrivalRate() {
        this(10_000, System::currentTimeMillis);
    }

    public ArrivalRate(long windowMs, LongSupplier clock) {
        this.windowMs = windowMs;
        this.clock = clock;
    }

    public void record() {
        long now = clock.getAsLong();
        arrivals.addLast(now);
        trim(now);
    }

    public double perSecond() {
        long now = clock.getAsLong();
        trim(now);
        return arrivals.size() * 1000.0 / windowMs;
    }

    private void trim(long now) {
        Long oldest;
        while ((oldest = arrivals.peekFirst()) != null && oldest < now - windowMs) {
            arrivals.pollFirst();
        }
    }
}
