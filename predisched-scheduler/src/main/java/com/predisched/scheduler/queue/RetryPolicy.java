package com.predisched.scheduler.queue;

import java.util.Random;

/**
 * Exponential backoff with jitter for failed attempts (F4, FR25).
 *
 * <pre>
 * delay(attempt) = min(maxDelayMs, baseDelayMs * 2^(attempt - 1)) adjusted by +/- jitter
 * </pre>
 *
 * <p>The jitter comes from a seeded {@link Random} (rule 8), so a benchmark replaying the same
 * trace with the same seed retries at the same moments.
 */
public class RetryPolicy {

    private final long baseDelayMs;
    private final long maxDelayMs;
    private final double jitterFraction;
    private final int defaultMaxRetries;
    private final Random random;

    public RetryPolicy(
            long baseDelayMs,
            long maxDelayMs,
            double jitterFraction,
            int defaultMaxRetries,
            long seed) {
        this.baseDelayMs = baseDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.jitterFraction = jitterFraction;
        this.defaultMaxRetries = defaultMaxRetries;
        this.random = new Random(seed);
    }

    /** How long to wait before try number {@code attempt + 1}, given {@code attempt} has failed. */
    public synchronized long delayMsAfter(int attempt) {
        int exponent = Math.max(0, attempt - 1);
        long raw = baseDelayMs;
        for (int i = 0; i < exponent && raw < maxDelayMs; i++) {
            raw *= 2;
        }
        long capped = Math.min(raw, maxDelayMs);
        if (jitterFraction <= 0) {
            return capped;
        }
        double jitter = (random.nextDouble() * 2 - 1) * jitterFraction * capped;
        return Math.max(0, Math.round(capped + jitter));
    }

    /** The retry limit for a task: its own when set, else the configured default. */
    public int maxRetriesFor(int taskMaxRetries) {
        return taskMaxRetries < 0 ? defaultMaxRetries : taskMaxRetries;
    }

    /** True while the task still has tries left. */
    public boolean shouldRetry(int attemptsSoFar, int taskMaxRetries) {
        return attemptsSoFar <= maxRetriesFor(taskMaxRetries);
    }

    public int defaultMaxRetries() {
        return defaultMaxRetries;
    }
}
