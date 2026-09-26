package com.predisched.scheduler.auth;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * A token bucket per client (F9): a bucket holds up to {@code burst} tokens, refills at
 * {@code ratePerSecond}, and every call takes one. A full bucket allows a burst; after that calls
 * are limited to the rate. Each bucket is guarded by its own monitor; the clock is injected so
 * tests control time.
 */
public class RateLimiter {

    private final LongSupplier nanoClock;
    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    public RateLimiter() {
        this(System::nanoTime);
    }

    public RateLimiter(LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
    }

    /** Takes a token for this client if one is available. */
    public boolean tryAcquire(String clientId, double ratePerSecond, int burst) {
        return buckets.computeIfAbsent(clientId, id -> new Bucket(burst, nanoClock.getAsLong()))
                .tryTake(nanoClock.getAsLong(), ratePerSecond, burst);
    }

    private static final class Bucket {
        private double tokens;
        private long lastNanos;

        Bucket(int burst, long now) {
            this.tokens = burst;
            this.lastNanos = now;
        }

        synchronized boolean tryTake(long now, double ratePerSecond, int burst) {
            tokens = Math.min(burst, tokens + (now - lastNanos) * ratePerSecond / 1e9);
            lastNanos = now;
            if (tokens >= 1) {
                tokens -= 1;
                return true;
            }
            return false;
        }
    }
}
