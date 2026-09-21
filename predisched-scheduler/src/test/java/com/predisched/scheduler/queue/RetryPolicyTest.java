package com.predisched.scheduler.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Backoff, jitter and the retry budget (F4, FR25). */
class RetryPolicyTest {

    @Test
    void delaysDoubleUntilTheCap() {
        RetryPolicy policy = new RetryPolicy(200, 5_000, 0, 3, 7);
        assertEquals(200, policy.delayMsAfter(1));
        assertEquals(400, policy.delayMsAfter(2));
        assertEquals(800, policy.delayMsAfter(3));
        assertEquals(1_600, policy.delayMsAfter(4));
        assertEquals(3_200, policy.delayMsAfter(5));
        assertEquals(5_000, policy.delayMsAfter(6), "capped at maxDelayMs");
        assertEquals(5_000, policy.delayMsAfter(20));
    }

    @Test
    void jitterStaysWithinItsFractionAndIsReproducible() {
        List<Long> first = delays(new RetryPolicy(1_000, 60_000, 0.2, 3, 42));
        List<Long> second = delays(new RetryPolicy(1_000, 60_000, 0.2, 3, 42));
        List<Long> other = delays(new RetryPolicy(1_000, 60_000, 0.2, 3, 43));

        assertEquals(first, second, "the same seed retries at the same moments (rule 8)");
        assertFalse(first.equals(other), "a different seed gives different jitter");

        long base = 1_000;
        assertTrue(first.get(0) >= base * 0.8 && first.get(0) <= base * 1.2,
                "attempt 1 within +/-20% of 1000 ms, was " + first.get(0));
    }

    private static List<Long> delays(RetryPolicy policy) {
        List<Long> out = new ArrayList<>();
        for (int attempt = 1; attempt <= 5; attempt++) {
            out.add(policy.delayMsAfter(attempt));
        }
        return out;
    }

    @Test
    void aTaskCanOverrideTheDefaultRetryLimit() {
        RetryPolicy policy = new RetryPolicy(100, 1_000, 0, 3, 1);
        assertEquals(3, policy.maxRetriesFor(-1), "unset falls back to the default");
        assertEquals(0, policy.maxRetriesFor(0), "a task can ask for no retries");
        assertEquals(7, policy.maxRetriesFor(7));
    }

    @Test
    void retriesRunOutAfterTheLimit() {
        RetryPolicy policy = new RetryPolicy(100, 1_000, 0, 2, 1);
        assertTrue(policy.shouldRetry(1, -1), "first failure: 1 of 2 retries");
        assertTrue(policy.shouldRetry(2, -1), "second failure: 2 of 2 retries");
        assertFalse(policy.shouldRetry(3, -1), "third failure is one too many");
    }
}
