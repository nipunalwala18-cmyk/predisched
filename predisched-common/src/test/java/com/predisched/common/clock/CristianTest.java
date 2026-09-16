package com.predisched.common.clock;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Cristian sync with a fake 40 ms RTT: a clock 1 s behind lands within RTT/2 of
 * server time.
 */
class CristianTest {

  @Test
  void resultWithinHalfRttOfServerTime() throws Exception {
    NodeClock local = new NodeClock(-1000, 0, Double.MAX_VALUE);
    CristianSync.Server server =
        () -> {
          Thread.sleep(40); // fake 40 ms RTT
          return System.currentTimeMillis();
        };

    var result = CristianSync.synchronize(local, server);
    long serverNow = System.currentTimeMillis();

    assertTrue(result.rttMs() >= 35, "rtt=" + result.rttMs());
    assertTrue(
        Math.abs(local.now() - serverNow) <= result.rttMs() / 2 + 8,
        "local=" + local.now() + " server=" + serverNow + " rtt=" + result.rttMs());
  }
}
