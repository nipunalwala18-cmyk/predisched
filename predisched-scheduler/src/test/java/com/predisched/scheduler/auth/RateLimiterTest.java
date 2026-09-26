package com.predisched.scheduler.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.auth.ClientDirectory;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class RateLimiterTest {

    private static final long SECOND = 1_000_000_000L;

    @Test
    void aBurstIsAllowedThenLimitedThenRefilled() {
        AtomicLong now = new AtomicLong(0);
        RateLimiter limiter = new RateLimiter(now::get);
        for (int i = 0; i < 5; i++) {
            assertTrue(limiter.tryAcquire("c", 2, 5), "burst call " + i);
        }
        assertFalse(limiter.tryAcquire("c", 2, 5), "bucket empty");

        now.addAndGet(SECOND / 2);          // half a second at 2/s = one token
        assertTrue(limiter.tryAcquire("c", 2, 5));
        assertFalse(limiter.tryAcquire("c", 2, 5));

        now.addAndGet(60 * SECOND);         // a long idle refills only up to the burst
        int allowed = 0;
        while (limiter.tryAcquire("c", 2, 5)) {
            allowed++;
        }
        assertEquals(5, allowed);
        assertTrue(limiter.tryAcquire("other", 2, 5), "clients have separate buckets");
    }

    @Test
    void theQuotaCountsUnfinishedTasksPerClient() {
        ClientLimits limits = new ClientLimits(new ClientDirectory(List.of(
                new ClientDirectory.Client("c", "hash", 10, 10, 3)), ""), new RateLimiter());
        assertNull(limits.reserve("c", 2));
        assertNull(limits.reserve("c", 1));
        String refused = limits.reserve("c", 1);
        assertTrue(refused != null && refused.contains("at most 3 unfinished tasks"), refused);
        limits.release("c", 1);
        assertNull(limits.reserve("c", 1));
        assertNull(limits.reserve("", 100), "no client id (auth off) is not limited");
    }
}
