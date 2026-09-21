package com.predisched.common.time;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lamport logical clock (lab Exp 3, FR8).
 *
 * <pre>
 * tick()            before a local event or a send: L = L + 1
 * update(received)  on receipt:                     L = max(L, received) + 1
 * </pre>
 *
 * <p>Thread-safe through a CAS loop, so concurrent ticks never hand out the same value.
 */
public class LamportClock {

    private final AtomicLong time;

    public LamportClock() {
        this(0L);
    }

    public LamportClock(long start) {
        this.time = new AtomicLong(start);
    }

    /** Advances the clock for a local event or an outgoing message, returning the new value. */
    public long tick() {
        return time.incrementAndGet();
    }

    /** Merges a received timestamp: max(local, received) + 1. Returns the new value. */
    public long update(long received) {
        while (true) {
            long local = time.get();
            long next = Math.max(local, received) + 1;
            if (time.compareAndSet(local, next)) {
                return next;
            }
        }
    }

    public long current() {
        return time.get();
    }

    @Override
    public String toString() {
        return "L=" + current();
    }
}
