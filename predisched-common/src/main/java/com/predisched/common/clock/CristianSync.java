package com.predisched.common.clock;

/**
 * Cristian's algorithm (the configurable alternative to Berkeley, run on workers):
 * request the server time, assume symmetric delay, set
 * {@code serverTime + RTT/2}.
 */
public class CristianSync {

  /** A time server; gRPC or fake (tests). */
  public interface Server {
    /** Returns the server's current time; the client measures the RTT around the call. */
    long queryTime() throws Exception;
  }

  public record Result(long rttMs, long offsetAppliedMs) {}

  public static Result synchronize(NodeClock local, Server server) throws Exception {
    long before = local.now();
    long serverTime = server.queryTime();
    long after = local.now();
    long rtt = Math.max(0, after - before);
    long offset = (serverTime + rtt / 2) - after;
    local.adjust(offset);
    return new Result(rtt, offset);
  }
}
