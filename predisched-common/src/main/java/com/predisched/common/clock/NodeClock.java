package com.predisched.common.clock;

/**
 * A node's wall clock: {@code now() = System.currentTimeMillis() + simulatedOffsetMs
 * + drift + correction}.
 *
 * <p>{@code simulatedOffsetMs} and {@code driftPpm} come from config so skew can be
 * demonstrated on one machine. Corrections from clock sync are slewed gradually at
 * {@code slewMsPerSec} and {@link #now()} never steps backwards (it returns at
 * least its previous value).
 *
 * <p>All product code reads wall time through this class, never
 * {@code System.currentTimeMillis()} directly.
 */
public class NodeClock {

  private final long simulatedOffsetMs;
  private final double driftPpm;
  private final double slewMsPerSec;
  private final long startRealtimeMs;
  private long lastSlewRealtimeMs;
  private long targetCorrectionMs;
  private long appliedCorrectionMs;
  private long lastReturned = Long.MIN_VALUE;

  public NodeClock(long simulatedOffsetMs, double driftPpm) {
    this(simulatedOffsetMs, driftPpm, 200.0);
  }

  /** @param slewMsPerSec correction slew rate; huge values apply almost instantly (tests). */
  public NodeClock(long simulatedOffsetMs, double driftPpm, double slewMsPerSec) {
    this.simulatedOffsetMs = simulatedOffsetMs;
    this.driftPpm = driftPpm;
    this.slewMsPerSec = slewMsPerSec;
    this.startRealtimeMs = System.currentTimeMillis();
    this.lastSlewRealtimeMs = startRealtimeMs;
  }

  public synchronized long now() {
    long realtime = System.currentTimeMillis();
    long elapsed = realtime - startRealtimeMs;
    // Slew the applied correction toward its target, bounded by elapsed * rate.
    long sinceSlew = Math.max(0, realtime - lastSlewRealtimeMs);
    lastSlewRealtimeMs = realtime;
    long remaining = targetCorrectionMs - appliedCorrectionMs;
    if (remaining != 0 && slewMsPerSec > 0) {
      long step = (long) (sinceSlew * slewMsPerSec / 1000.0);
      if (remaining > 0) {
        appliedCorrectionMs += Math.min(remaining, step);
      } else {
        appliedCorrectionMs += Math.max(remaining, -step);
      }
    } else if (slewMsPerSec <= 0) {
      appliedCorrectionMs = targetCorrectionMs;
    }
    long computed =
        realtime + simulatedOffsetMs + (long) (elapsed * driftPpm / 1_000_000.0)
            + appliedCorrectionMs;
    if (computed < lastReturned) {
      return lastReturned;
    }
    lastReturned = computed;
    return computed;
  }

  /** Request a clock correction; applied gradually by {@link #now()}. */
  public synchronized void adjust(long offsetMs) {
    targetCorrectionMs += offsetMs;
  }

  public synchronized long correctionTarget() {
    return targetCorrectionMs;
  }

  public synchronized long appliedCorrection() {
    return appliedCorrectionMs;
  }
}
